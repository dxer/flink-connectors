package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.util.RedisUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class RedisTableSource implements StreamTableSource<Row>, LookupableTableSource<Row> {

    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;
    private final ReadableConfig options;


    public RedisTableSource(ReadableConfig options, String[] fieldNames, TypeInformation[] fieldTypes) {
        this.options = options;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return null;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        JedisConfigBase jedisConfig = RedisUtils.getJedisConfig(options);

        String mode = options.get(RedisDynamicTableFactory.REDIS_MODE);

        Boolean cacheEnable = options.get(RedisDynamicTableFactory.REDIS_CACHE);
        Integer cacheSize = options.get(RedisDynamicTableFactory.REDIS_CACHE_SIZE);
        Integer cacheTTL = options.get(RedisDynamicTableFactory.REDIS_CACHE_TTL);
        Boolean cacheEmpty = options.get(RedisDynamicTableFactory.REDIS_CACHE_EMPTY);

        return RedisLookupFunction.builder()
                .setJedisConfigBase(jedisConfig)
                .setModeType(ModeType.fromMode(mode))
                .setCacheEnable(cacheEnable)
                .setCacheSize(cacheSize)
                .setCacheTTL(cacheTTL)
                .setCacheEmpty(cacheEmpty)
                .build();
    }

    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        throw new IllegalArgumentException("Redis DataStream is not supported");
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }
}
