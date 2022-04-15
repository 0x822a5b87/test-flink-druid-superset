package com.xxx.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * @author 0x822a5b87
 */
public class OverloadedFunction extends ScalarFunction {
    /**
     * no hint required
     */
    public Long eval(long a, long b) {
        return a + b;
    }

    /**
     * define the precision and scale of a decimal
     */
    public @DataTypeHint("DECIMAL(12, 3)") BigDecimal eval(double a, double b) {
        return BigDecimal.valueOf(a + b);
    }

    /**
     * define a nested data type
     */
    @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
    public Row eval(int i) {
        return Row.of(String.valueOf(i), Instant.ofEpochSecond(i));
    }

    /**
     * allow wildcard input and customly serialized output
     */
    @DataTypeHint(value = "RAW", bridgedTo = ByteBuffer.class)
    public ByteBuffer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return ByteBuffer.wrap(String.valueOf(o).getBytes());
    }
}
