package com.xxx.flink.word.factory.serializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author 0x822a5b87
 */
public class TglogSerializer implements SerializationSchema<RowData> {

    private final String identifierName;

    private final String columnDelimiter;

    private final DataType physicalDataType;

    public TglogSerializer(String identifierName, String columnDelimiter, DataType physicalDataType) {
        this.identifierName   = identifierName;
        this.columnDelimiter  = columnDelimiter;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(RowData element) {
        RowType       logicalType = (RowType) physicalDataType.getLogicalType();
        StringBuilder sb          = new StringBuilder(identifierName);
        sb.append(columnDelimiter);
        for (RowType.RowField field : logicalType.getFields()) {
            int fieldIndex = logicalType.getFieldIndex(field.getName());
            sb.append(field.getName());
            sb.append(columnDelimiter);
        }
        return sb.toString().getBytes();
    }
}
