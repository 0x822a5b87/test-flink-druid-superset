package com.xxx.flink.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author 0x822a5b87
 */
public class Top2Function extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Function.Top2Accumulator> {

    @Override
    public Top2Accumulator createAccumulator() {
        Top2Accumulator acc = new Top2Accumulator();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accumulator acc, Integer value) {
        if (value > acc.first) {
            acc.second = acc.first;
            acc.first = value;
        } else if (value > acc.second) {
            acc.second = value;
        }
    }

    public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
        for (Top2Accumulator otherAcc : it) {
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }

    public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
        // emit the value and rank
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }

    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }


    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment    env      = TableEnvironment.create(settings);
        env.executeSql("CREATE TABLE MyTable (\n"
                       + "    `id` STRING,\n"
                       + "    `name` STRING,\n"
                       + "    `price` int,\n"
                       + "    `type` int"
                       + ") WITH (\n"
                       + "    'connector' = 'kafka',\n"
                       + "    'topic' = 'test',\n"
                       + "    'properties.bootstrap.servers' = '9.134.115.20:9092',\n"
                       + "\n"
                       + "    'properties.group.id' = 'testGroup',\n"
                       + "    'scan.startup.mode' = 'latest-offset',\n"
                       + "    'json.fail-on-missing-field' = 'false',\n"
                       + "    'json.ignore-parse-errors' = 'true',\n"
                       + "    'format' = 'json'\n"
                       + ")");

        env.createTemporarySystemFunction("top2", new Top2Function());

//        env.executeSql("SELECT *\n"
//                       + "FROM (\n"
//                       + "  SELECT *,\n"
//                       + "    ROW_NUMBER() OVER (PARTITION BY name ORDER BY type DESC) AS row_num\n"
//                       + "  FROM MyTable)\n"
//                       + "WHERE row_num <= 5")
//           .print();

        env.executeSql("SELECT myField,value,rank\n"
                       + "FROM MyTable\n"
                       + "GROUP BY myField\n"
                       + "AGG BY TOP2(value) as (value,rank)");

//        env
//                .from("MyTable")
//                .groupBy($("name"))
//                .flatAggregate(call(Top2Function.class, $("price")).as("value", "rank"))
//                .select($("name"), $("value"), $("rank"))
//                .execute()
//                .print();
    }
}
