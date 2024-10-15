package org.accenture.acciotov.kafka.transformer;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class CustomKey<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final ConfigDef CONFIG_DEF;
    private String prefix;

    static {
        CONFIG_DEF = (new ConfigDef()).define("prefix", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Redis key prefix to add.");
    }

    @Override
    public R apply(R record) {

        // modify the record key because we can
        String newKey = prefix + record.key().toString();

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                Schema.STRING_SCHEMA,
                newKey,
                record.valueSchema(),
                record.value(),
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.prefix = config.getString("prefix");
    }
}