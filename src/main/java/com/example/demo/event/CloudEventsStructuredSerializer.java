package com.example.demo.event;

import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;

import java.util.Map;

public class CloudEventsStructuredSerializer extends CloudEventSerializer {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        Map<String, Object> cloudEventConfigs = (Map<String, Object>) configs;
        cloudEventConfigs.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.STRUCTURED);
        cloudEventConfigs.put(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);

        super.configure(cloudEventConfigs, isKey);

    }

}
