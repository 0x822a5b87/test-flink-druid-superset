package com.xxx.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author 0x822a5b87
 */
public class MyConcatFunction extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields) {
        return Arrays.stream(fields)
                     .map(Object::toString)
                     .collect(Collectors.joining(","));
    }

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment    env      = TableEnvironment.create(settings);
        env.executeSql("CREATE TABLE MyTable (\n"
                       + "                            `name` STRING\n,"
                       + "                            `addr` STRING\n"
                       + ") WITH (\n"
                       + "    'connector' = 'kafka',\n"
                       + "    'topic' = 'test',\n"
                       + "    'properties.bootstrap.servers' = 'test.dc.data.woa.com:9092',\n"
                       + "\n"
                       + "    'properties.group.id' = 'testGroup',\n"
                       + "    'scan.startup.mode' = 'latest-offset',\n"
                       + "    'json.fail-on-missing-field' = 'false',\n"
                       + "    'json.ignore-parse-errors' = 'true',\n"
                       + "    'format' = 'json'\n"
                       + ")\n");

        env.createTemporarySystemFunction("MyConcatFunction", new MyConcatFunction());

        env.executeSql("select * from MyTable")
           .print();
    }

}
