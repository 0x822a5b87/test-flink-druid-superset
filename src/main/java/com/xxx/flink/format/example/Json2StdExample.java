package com.xxx.flink.format.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/**
 * @author 0x822a5b87
 */
public class Json2StdExample {

    public static void main(String[] args) throws Exception {
        Json2StdExample example = new Json2StdExample();
        example.work();
    }

    private void work() throws IOException, ExecutionException, InterruptedException {
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);
        createTable(env);
        insertData(env);
        env.executeSql("insert into sales_tglog\n"
                       + "select * from sales");
    }

    private void insertData(TableEnvironment env) throws ExecutionException, InterruptedException {
        // insert some example data into the table
        final TableResult insertionResult =
                env.executeSql(
                        "INSERT INTO sales VALUES"
                        + "  (1, 'Latte', 6), "
                        + "  (2, 'Milk', 3), "
                        + "  (3, 'Breve', 5), "
                        + "  (4, 'Mocha', 8), "
                        + "  (5, 'Tea', 4)"
                );

        insertionResult.await();
    }

    private void createTable(TableEnvironment env) throws IOException {
        final String salesDirPath = createTemporaryDirectory();
        env.executeSql(
                "CREATE TABLE sales ("
                + "  id int,"
                + "  name STRING,"
                + "  price INT"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '"
                + salesDirPath
                + "',"
                + "  'format' = 'csv'"
                + ")");

        env.executeSql(" " +
                       " CREATE TABLE sales_tglog ( " +
                       "  id int," +
                       "  name STRING," +
                       "  price INT" +
                       " ) WITH ( " +
                       "  'connector' = 'kafka', " +
                       "  'topic' = 'test_sender', " +
                       "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
                       "  'properties.enable.auto.commit' = 'false', " +
                       "  'properties.session.timeout.ms' = '90000', " +
                       "  'properties.request.timeout.ms' = '325000', " +
                       "  'format' = 'tglog', " +
                       "  'tglog.column-delimiter' = ',', " +
                       "  'sink.partitioner' = 'round-robin', " +
                       "  'sink.parallelism' = '4' " +
                       " ) "
        );

    }

    private static String createTemporaryDirectory() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("sales");
        return tempDirectory.toString();
    }
}
