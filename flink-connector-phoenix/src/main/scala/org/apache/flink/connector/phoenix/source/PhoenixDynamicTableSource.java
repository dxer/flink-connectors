package org.apache.flink.connector.phoenix.source;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.connector.phoenix.Constans;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * PhoenixDynamicTableSource
 */
public class PhoenixDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final JdbcOptions options;
    private final TableSchema physicalSchema;
    private final JdbcLookupOptions lookupOptions;

    public PhoenixDynamicTableSource(JdbcOptions options,
                                     JdbcLookupOptions lookupOptions,
                                     TableSchema physicalSchema) {
        this.options = options;
        this.lookupOptions = lookupOptions;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    /**
     * 全表
     *
     * @param scanContext
     * @return
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final JdbcDialect dialect = options.getDialect();

        String query = dialect.getSelectFromStatement(
                options.getTableName(),
                physicalSchema.getFieldNames(),
                new String[0]
        );

        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        final JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
                .setDrivername(Constans.PHOENIX_DRIVER)
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setQuery(query)
                .setRowConverter(dialect.getRowConverter(rowType))
                .setRowDataTypeInfo(scanContext.createTypeInformation(physicalSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());
    }

    /**
     * 根据条件去查询
     *
     * @param context
     * @return
     */
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keysScanTableSource
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1,
                    "JDBC only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        return TableFunctionProvider.of(new PhoenixRowDataLookupFunction(
                options,
                lookupOptions,
                physicalSchema.getFieldNames(),
                physicalSchema.getFieldDataTypes(),
                keyNames,
                rowType));
    }

    @Override
    public DynamicTableSource copy() {
        return new PhoenixDynamicTableSource(options, lookupOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Phoenix Table Source";
    }


}
