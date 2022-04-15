package com.xxx.flink.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author 0x822a5b87
 */
public class AnotherSubstringFunction extends ScalarFunction {
    private final boolean endInclusive;

    public AnotherSubstringFunction(boolean endInclusive) {
        this.endInclusive = endInclusive;
    }

    public String eval(String s, Integer begin, Integer end) {
        return StringUtils.substring(s, begin, endInclusive ? end + 1 : end);
    }

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment    env      = TableEnvironment.create(settings);
        env.executeSql("CREATE TABLE MyTable (\n"
                       + "                            `name` STRING\n"
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

        env.createTemporarySystemFunction("SubstringFunction2", new AnotherSubstringFunction(true));

        env.executeSql("select SubstringFunction2(name, 0, 10) from MyTable")
           .print();
    }
}
