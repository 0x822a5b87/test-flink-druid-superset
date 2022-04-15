package com.xxx.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author 0x822a5b87
 * function with overloaded evaluation methods but globally defined output type
 */
@FunctionHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
public class OverloadedFunction2 extends TableFunction<Row> {

    public void eval(int a, int b) {
        collect(Row.of("Sum", a + b));
    }

    public void eval() {
        collect(Row.of("Empty args", -1));
    }
}
