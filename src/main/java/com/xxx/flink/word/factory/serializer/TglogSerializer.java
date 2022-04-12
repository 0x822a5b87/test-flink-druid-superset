package com.xxx.flink.word.factory.serializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author 0x822a5b87
 */
public class TglogSerializer implements SerializationSchema<RowData> {

    private final String identifierName;

    private final String columnDelimiter;

    private final DataType physicalDataType;

    private RowData.FieldGetter[] fieldGetters;

    public TglogSerializer(String identifierName, String columnDelimiter, DataType physicalDataType) {
        this.identifierName   = identifierName;
        this.columnDelimiter  = columnDelimiter;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
        RowType logicalType = (RowType) physicalDataType.getLogicalType();
        int     fieldCount  = logicalType.getFieldCount();
        final LogicalType[] fieldTypes =
                logicalType.getFields().stream()
                           .map(RowType.RowField::getType)
                           .toArray(LogicalType[]::new);
        fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
    }

    @Override
    public byte[] serialize(RowData element) {
        RowType      type       = (RowType) physicalDataType.getLogicalType();
        StringBuilder sb        = new StringBuilder(identifierName);
        sb.append(columnDelimiter);
        for (int i = 0; i < type.getFieldCount(); i++) {
            Object        fieldOrNull = fieldGetters[i].getFieldOrNull(element);
            sb.append(fieldOrNull == null ? "" : fieldOrNull.toString())
              .append(columnDelimiter);
        }
        return sb.toString().getBytes();
    }
}
