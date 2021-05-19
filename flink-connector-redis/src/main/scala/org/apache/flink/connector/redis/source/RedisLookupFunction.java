package org.apache.flink.connector.redis.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.redis.ModeType;
import org.apache.flink.connector.redis.client.JedisConfigBase;
import org.apache.flink.connector.redis.client.RedisClientBase;
import org.apache.flink.connector.redis.client.RedisClientBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The RedisLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL. It looks up the result as {@link Row}.
 */
@Internal
public class RedisLookupFunction extends AsyncTableFunction<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private final JedisConfigBase jedisConfigBase;
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;
    private final ModeType modeType;

    private Boolean cacheEnable;
    private Integer cacheSize;
    private Integer cacheTTL;
    private Boolean cacheEmpty;


    private transient RedisClientBase redisClient;
    private RedisSourceOperator redisOperator;

    /**
     * 缓存
     */
    private transient Cache<String, String> cacheString;
    private transient Cache<String, Map<String, String>> cacheHash;

    public RedisLookupFunction(JedisConfigBase jedisConfigBase, String[] fieldNames, TypeInformation[] fieldTypes,
                               ModeType modeType, Boolean cacheEnable, Integer cacheSize, Integer cacheTTL, Boolean cacheEmpty) {
        this.jedisConfigBase = jedisConfigBase;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.modeType = modeType;
        this.cacheEnable = cacheEnable;
        this.cacheSize = cacheSize;
        this.cacheTTL = cacheTTL;
        this.cacheEmpty = cacheEmpty;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            if (null == redisClient) {
                this.redisClient = RedisClientBuilder.build(this.jedisConfigBase);
            }
            redisOperator = new RedisSourceOperator(redisClient);

            switch (modeType) {
                case HASHMAP:
                    this.cacheHash = !cacheEnable ? null : CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTTL, TimeUnit.SECONDS)
                            .maximumSize(cacheSize)
                            .build();
                    break;
                case STRING:
                    this.cacheString = !cacheEnable ? null : CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTTL, TimeUnit.SECONDS)
                            .maximumSize(cacheSize)
                            .build();
                    break;
                default:
                    throw new Exception("");

            }

        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    public void eval(CompletableFuture<Collection<Row>> future, String key) throws Exception {
        switch (modeType) {
            case HASHMAP:
                if (cacheHash != null) {
                    Map<String, String> value = cacheHash.getIfPresent(key);
                    if (value != null) {
                        future.complete(Collections.singletonList(Row.of(key, value)));
                        return;
                    } else {
                        value = redisOperator.hgetAll(key);
                        if (value == null) {
                            value.put("", "");
                        }
                        if (cacheHash != null) {
                            cacheHash.put(key, value);
                        }
                        future.complete(Collections.singletonList(Row.of(key, value)));
                    }
                }
                break;
            case STRING:
                if (cacheString != null) {
                    String value = cacheString.getIfPresent(key);
                    if (value != null) {
                        future.complete(Collections.singleton(Row.of(key, value)));
                        return;
                    } else {
                        value = redisOperator.get(key);
                        value = value == null ? "" : value;
                        if (cacheString != null) {
                            cacheString.put(key, value);
                        }
                        future.complete(Collections.singleton(Row.of(key, value)));
                    }
                }
                break;
            default:
                throw new Exception("");
        }
    }

    /**
     * 返回类型
     *
     * @return
     */
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void close() throws Exception {
        if (redisClient != null) {
            redisClient.close();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;
        private JedisConfigBase jedisConfigBase;
        private ModeType modeType;
        private Boolean cacheEnable;
        private Integer cacheSize;
        private Integer cacheTTL;
        private Boolean cacheEmpty;


        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setJedisConfigBase(JedisConfigBase jedisConfigBase) {
            this.jedisConfigBase = jedisConfigBase;
            return this;
        }

        public Builder setModeType(ModeType modeType) {
            this.modeType = modeType;
            return this;
        }

        public Builder setCacheEnable(Boolean cacheEnable) {
            this.cacheEnable = cacheEnable;
            return this;
        }

        public Builder setCacheSize(Integer cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder setCacheTTL(Integer cacheTTL) {
            this.cacheTTL = cacheTTL;
            return this;
        }

        public Builder setCacheEmpty(Boolean cacheEmpty) {
            this.cacheEmpty = cacheEmpty;
            return this;
        }

        public RedisLookupFunction build() {
            return new RedisLookupFunction(
                    jedisConfigBase,
                    fieldNames,
                    fieldTypes,
                    modeType,
                    cacheEnable,
                    cacheSize,
                    cacheTTL,
                    cacheEmpty);
        }

    }
}
