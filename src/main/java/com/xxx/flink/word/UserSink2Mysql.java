package com.xxx.flink.word;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ResolvedSchema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 0x822a5b87
 */
public class UserSink2Mysql {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment    tEnv     = TableEnvironment.create(settings);
        TableResult tableResult = tEnv.executeSql("CREATE TABLE kafka_source (\n" +
                                                  "                            `user_id` BIGINT,\n" +
                                                  "                            `item_id` BIGINT,\n" +
                                                  "                            `behavior` STRING\n" +
                                                  // "                            `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                                                  ") WITH (\n" +
                                                  "    'connector' = 'kafka',\n" +
                                                  "    'topic' = 'test',\n" +
                                                  "    'properties.bootstrap.servers' = 'test.dc.data.woa.com:9092'," +
                                                  "\n" +
                                                  "    'properties.group.id' = 'testGroup',\n" +
                                                  "    'scan.startup.mode' = 'latest-offset',\n" +
                                                  "    'json.fail-on-missing-field' = 'false'," +
                                                  "    'json.ignore-parse-errors' = 'true'," +
                                                  "    'format' = 'json'\n" +
                                                  ')');
        Table kafkaJsonSource = tEnv.from("kafka_source");
//        tEnv.executeSql("CREATE TABLE print_table WITH ('connector' = 'print')\n" +
//                        "LIKE kafka_source (EXCLUDING ALL)");

        TableResult result = kafkaSink(tEnv);
        kafkaJsonSource
                .select($("user_id"),
                        $("item_id"),
                        $("behavior")
                )
                .executeInsert("kafka_sink2_kafka_table")
                .print();
    }

    private static TableResult kafkaSink(TableEnvironment tableEnvironment) {
        return tableEnvironment.executeSql("CREATE TABLE kafka_sink2_kafka_table (\n"
                                           + "                            `user_id` BIGINT,\n"
                                           + "                            `item_id` BIGINT,\n"
                                           + "                            `behavior` STRING\n"
                                           + ") WITH (\n"
                                           + "    'connector' = 'kafka',\n"
                                           + "    'topic' = 'test_output',\n"
                                           + "    'properties.bootstrap.servers' = 'test.dc.data.woa.com:9092',\n"
                                           + "    'format' = 'tglog'\n"
                                           + ")");
    }

    private static TableResult mysqlSink(TableEnvironment tableEnvironment) {
        return tableEnvironment.executeSql("CREATE TABLE kafka_sink_table\n" +
                                           "(\n" +
                                           "    `user_id` BIGINT,\n" +
                                           "    `item_id` BIGINT,\n" +
                                           "    `behavior` STRING\n" +
                                           ")\n" +
                                           "WITH (\n" +
                                           "    'connector' = 'jdbc',\n" +
                                           "    'url' = 'jdbc:mysql://127.0.0.1:3306/test',\n" +
                                           "    'username'= 'root',\n" +
                                           "    'password'= '123456',\n" +
                                           "    'table-name' = 'kafka_sink_table'\n" +
                                           ")");
    }
}

