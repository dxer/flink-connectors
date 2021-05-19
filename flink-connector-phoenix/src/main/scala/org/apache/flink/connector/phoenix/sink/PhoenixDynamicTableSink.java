package org.apache.flink.connector.phoenix.sink;

import org.apache.flink.connector.phoenix.PhoenixOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 *
 */
public class PhoenixDynamicTableSink implements DynamicTableSink {

    private PhoenixOptions phoenixOptions;

    private DataType[] fieldDataTypes;

    private String[] fieldNames;

    private int[] keyIndices;

    public PhoenixDynamicTableSink(PhoenixOptions phoenixOptions, String[] fieldNames, DataType[] fieldDataTypes, int[] keyIndices) {
        this.phoenixOptions = phoenixOptions;
        this.fieldDataTypes = fieldDataTypes;
        this.fieldNames = fieldNames;
        this.keyIndices = keyIndices;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        PhoenixSinkFunction sinkFunction = new PhoenixSinkFunction(phoenixOptions, fieldNames, fieldDataTypes, keyIndices);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new PhoenixDynamicTableSink(phoenixOptions, fieldNames, fieldDataTypes, keyIndices);
    }

    @Override
    public String asSummaryString() {
        return "Phoenix Table Sink";
    }
}
