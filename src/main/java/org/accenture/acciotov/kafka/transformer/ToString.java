package org.accenture.acciotov.kafka.transformer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
public abstract class ToString<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        Schema schema = this.operatingSchema(record);
        String value = this.operatingValue(record).toString();
        return this.newRecord(record, schema, value);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Nothing to configure
    }

    protected abstract Schema operatingSchema(R var1);

    protected abstract Object operatingValue(R var1);

    protected abstract R newRecord(R var1, Schema var2, Object var3);

    public static class Value<R extends ConnectRecord<R>> extends ToString<R> {
        public Value() {
        }
        @Override
        protected Schema operatingSchema(R record) {
            return Schema.STRING_SCHEMA;
        }
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends ToString<R> {
        public Key() {
        }

        @Override
        protected Schema operatingSchema(R record) {
            return Schema.STRING_SCHEMA;
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }
}