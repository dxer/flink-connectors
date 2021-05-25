package org.apache.flink.connector.phoenix;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
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

import java.time.Duration;
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

    // look up config options
    private static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .longType()
            .defaultValue(-1L)
            .withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " +
                    "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " +
                    "specified. Cache is not enabled as default.");
    private static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
            .key("lookup.cache.ttl")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("the cache time to live.");
    private static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
            .key("lookup.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if lookup database failed.");

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
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        JdbcLookupOptions lookupOptions = getJdbcLookupOptions(helper.getOptions());
        return new PhoenixDynamicTableSource(jdbcOptions, lookupOptions, physicalSchema);
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
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_MAX_RETRIES);
        return set;
    }

    private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String serverUrl = readableConfig.get(SERVER_URL);

        final JdbcOptions.Builder builder = JdbcOptions.builder()
                .setDriverName(Constans.PHOENIX_DRIVER)
                .setDBUrl(String.format(Constans.PHOENIX_JDBC_URL_TEMPLATE, serverUrl)) // 组装成 phoenix jdbc thin url
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(new PhoenixDialect());

        readableConfig.getOptional(JdbcDynamicTableFactory.USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(JdbcDynamicTableFactory.PASSWORD).ifPresent(builder::setPassword);

        return builder.build();
    }

    private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
        return new JdbcLookupOptions(
                readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
                readableConfig.get(LOOKUP_CACHE_TTL).toMillis(),
                readableConfig.get(LOOKUP_MAX_RETRIES));
    }
}
