package org.apache.flink.connector.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.sink.RedisUpsertTableSink;
import org.apache.flink.connector.redis.source.RedisTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

public class RedisTableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        return new RedisTableSource(null, tableSchema.getFieldNames(), tableSchema.getFieldTypes());
    }

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        return new RedisUpsertTableSink(getValidatedProperties(properties));
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        return descriptorProperties;
    }


    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();

        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        return properties;
    }

}
