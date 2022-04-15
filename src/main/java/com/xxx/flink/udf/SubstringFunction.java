package com.xxx.flink.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author 0x822a5b87
 */
public class SubstringFunction extends ScalarFunction {
    /**
     * define function logic
     */
    public static class SubstringFunction2 extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return StringUtils.substring(s, begin, end);
        }
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment execEnv    = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment     tableEnv   = StreamTableEnvironment.create(execEnv);
        DataStreamSource<String>   dataSource = execEnv.fromCollection(Arrays.asList("hello", "world", "1", "a long sentence from SubstringFunction"));

        Schema schema = Schema.newBuilder().columnByExpression("myField", "f0").build();
        Table  table  = tableEnv.fromDataStream(dataSource, schema);

        // call function "inline" without registration in Table API
        table.select(call(SubstringFunction2.class, $("myField"), 5, 12));

        // register function
        tableEnv.createTemporarySystemFunction("SubstringFunction2", SubstringFunction2.class);

        // call registered function in Table API
        table.select(call("SubstringFunction2", $("myField"), 5, 12));

        // call registered function in SQL
        tableEnv.sqlQuery("SELECT SubstringFunction2(myField, 5, 12) FROM " + table)
                .execute()
                .print();
    }
}
