package com.example.demo.event;

import io.cloudevents.core.message.Encoding;
import io.cloudevents.kafka.CloudEventSerializer;

import java.util.Map;

public class CloudEventsBinarySerializer extends CloudEventSerializer {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        Map<String, Object> cloudEventConfigs = (Map<String, Object>) configs;
        cloudEventConfigs.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.BINARY);

        super.configure(cloudEventConfigs, isKey);

    }

}
