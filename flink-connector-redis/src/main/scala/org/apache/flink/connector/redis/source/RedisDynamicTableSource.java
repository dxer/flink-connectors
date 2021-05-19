package org.apache.flink.connector.redis.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

public class RedisDynamicTableSource implements ScanTableSource, LookupTableSource {

    public RedisDynamicTableSource() {

    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return null;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
