package com.xxx.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author 0x822a5b87
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitFunction extends TableFunction<Row> {

    public static final String SEPARATOR = " ";

    public void eval(String str) {
        for (String s : str.split(SEPARATOR)) {
            // use collect(...) to emit a row
            collect(Row.of(s, s.length()));
        }
    }
}
