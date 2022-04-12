package com.xxx.flink.word.factory.format;

import com.xxx.flink.word.factory.serializer.TglogSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * @author 0x822a5b87
 */
public class TglogEncodeFormat implements EncodingFormat<SerializationSchema<RowData>> {

    private final String identifierName;
    private final String columnDelimiter;

    public TglogEncodeFormat(String identifierName, String columnDelimiter) {
        this.identifierName  = identifierName;
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType physicalDataType) {

        return new TglogSerializer(identifierName, columnDelimiter, physicalDataType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                            .addContainedKind(RowKind.INSERT)
                            .build();
    }
}
