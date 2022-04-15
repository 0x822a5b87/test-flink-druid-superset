package com.xxx.flink.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Optional;

/**
 * @author 0x822a5b87
 */
public class LiteralFunction extends ScalarFunction {
    public Object eval(String s, String type) {
        switch (type) {
            case "INT":
                return Integer.valueOf(s);
            case "DOUBLE":
                return Double.valueOf(s);
            case "STRING":
            default:
                return s;
        }
    }

    /**
     * the automatic, reflection-based type inference is disabled and replaced by the following logic
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                            // specify typed arguments
                            // parameters will be casted implicitly to those types if necessary
                            .typedArguments(DataTypes.STRING(), DataTypes.STRING())
                            // specify a strategy for the result data type of the function
                            .outputTypeStrategy(callContext -> {
                                if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
                                    throw callContext.newValidationError("Literal expected for second argument.");
                                }
                                // return a data type based on a literal
                                final String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
                                switch (literal) {
                                    case "INT":
                                        return Optional.of(DataTypes.INT().notNull());
                                    case "DOUBLE":
                                        return Optional.of(DataTypes.DOUBLE().notNull());
                                    case "STRING":
                                    default:
                                        return Optional.of(DataTypes.STRING());
                                }
                            })
                            .build();
    }
}
