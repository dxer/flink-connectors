package org.apache.flink.connector.phoenix;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.connector.phoenix.sink.PhoenixDynamicTableSink;
import org.apache.flink.connector.phoenix.source.PhoenixDialect;
import org.apache.flink.connector.phoenix.source.PhoenixDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 *
 */
public class PhoenixDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "Phoenix5";

    private static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of Phoenix table to connect.");

    private static final ConfigOption<String> SERVER_URL = ConfigOptions
            .key("server-url")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of Phoenix table to connect.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        int[] keyIndices = TableSchemaUtils.getPrimaryKeyIndices(tableSchema);

        DataType[] fieldDataTypes = tableSchema.getFieldDataTypes(); // 获取字段类型
        String[] fieldNames = tableSchema.getFieldNames(); // 获取字段名

        ReadableConfig options = helper.getOptions();
        PhoenixOptions phoenixOptions = new PhoenixOptions(options.get(SERVER_URL), options.get(TABLE_NAME));

        return new PhoenixDynamicTableSink(phoenixOptions, fieldNames, fieldDataTypes, keyIndices);
    }


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();
        JdbcOptions jdbcOptions = getJdbcOptions(config);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new PhoenixDynamicTableSource(jdbcOptions, schema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_NAME);
        set.add(SERVER_URL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_NAME);
        set.add(SERVER_URL);
        return set;
    }

    private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String serverUrl = readableConfig.get(SERVER_URL);

        final JdbcOptions.Builder builder = JdbcOptions.builder()
                .setDriverName(Constans.PHOENIX_DRIVER)
                .setDBUrl(String.format(Constans.PHOENIX_JDBC_URL_TEMPLATE, serverUrl))
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(new PhoenixDialect());

        readableConfig.getOptional(JdbcDynamicTableFactory.USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(JdbcDynamicTableFactory.PASSWORD).ifPresent(builder::setPassword);

        return builder.build();

    }
}
