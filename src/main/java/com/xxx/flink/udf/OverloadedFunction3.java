package com.xxx.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author 0x822a5b87
 * decouples the type inference from evaluation methods,
 * the type inference is entirely determined by the function hints
 */
@FunctionHint(
        input = {@DataTypeHint("INT"), @DataTypeHint("INT")},
        output = @DataTypeHint("INT")
)
@FunctionHint(
        input = {@DataTypeHint("BIGINT"), @DataTypeHint("BIGINT")},
        output = @DataTypeHint("BIGINT")
)
@FunctionHint(
        input = {},
        output = @DataTypeHint("BOOLEAN")
)
public class OverloadedFunction3 extends TableFunction<Row> {
    /**
     * an implementer just needs to make sure that a method exists that can be called by the JVM
     */
    public boolean eval(Object... o) {
        System.out.println("test");
        collect(Row.of(false));
        return false;
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

        env.createTemporarySystemFunction("OverloadedFunction3", new OverloadedFunction3());

        env.executeSql("select OverloadedFunction3() from MyTable")
           .print();
    }

}
