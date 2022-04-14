package com.xxx.flink.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;


/**
 * @author 0x822a5b87
 */
public class SpecifyingQuery {

    public static void main(String[] args) {
        ObjectMapper               mapper   = new ObjectMapper();
        StreamExecutionEnvironment env      = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment     tableEnv = StreamTableEnvironment.create(env);

        // ingest a DataStream from an external source
        DataStream<Tuple3<Integer, String, Integer>> dataSource
                = env.socketTextStream("localhost", 9999)
                     .map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
                         @Override
                         public Tuple3<Integer, String, Integer> map(String message) throws Exception {
                             Map<String, Object> map = mapper.readValue(message, Map.class);

                             return new Tuple3<>(
                                     (Integer) map.get("user"),
                                     (String) map.get("product"),
                                     (Integer) map.get("amount"));
                         }
                     });


        // SQL queries with an inlined (unregistered) table
        Schema schema = Schema.newBuilder()
                              .columnByExpression("user", "f0")
                              .columnByExpression("product", "f1")
                              .columnByExpression("amount", "f2")
                              .build();
        Table table = tableEnv.fromDataStream(dataSource, schema);
        Table result = tableEnv.sqlQuery(
                "SELECT SUM(amount) as total_amount FROM " + table + " WHERE product LIKE '%Rubber%'");
        result.execute().print();
    }

}
