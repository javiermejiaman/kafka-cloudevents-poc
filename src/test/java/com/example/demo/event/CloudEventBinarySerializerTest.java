package com.example.demo.event;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CloudEventBinarySerializerTest {

    private CloudEventsBinarySerializer binarySerializer;

    private String topic;
    private RecordHeaders headers;
    private CloudEvent message;

    private String testData;
    private Map<String, Object> testConfig;

    @BeforeEach
    void setUp() throws URISyntaxException {
        binarySerializer = getSerializer();

        testData = getTestData();
        testConfig = getTestConfig();

        topic = getTopic();
        headers = getHeaders();
        message = getMessage();
    }

    @AfterEach
    void tearDown() {
        binarySerializer = null;

        testData = null;
        testConfig = null;

        topic = null;
        headers = null;
        message = null;
    }

    @Test
    void binaryEncodingTest() {

        binarySerializer.configure(testConfig, false);

        byte[] serialized = binarySerializer.serialize(topic, headers, message);

        assertThat(new String(serialized, StandardCharsets.UTF_8)).isEqualTo(testData);

    }

    @Test
    void assetThrowsKeySerializerExceptionTest() {
        assertThrows(
                IllegalArgumentException.class,
                () -> binarySerializer.configure(testConfig, true)
        );
    }

    @Test
    void assertNoThrowsValueSerializerTest() {
        assertDoesNotThrow(() -> binarySerializer.configure(testConfig, false));
    }

    private CloudEventsBinarySerializer getSerializer() {
        return new CloudEventsBinarySerializer();
    }

    private String getTestData() {
        return "{\"key\": \"value\"}";
    }

    private Map<String, Object> getTestConfig() {
        return new HashMap<>();
    }

    private String getTopic() {
        return "test.topic";
    }

    private RecordHeaders getHeaders() {
        return new RecordHeaders();
    }

    private CloudEvent getMessage() throws URISyntaxException {
        return new CloudEventBuilder()
                .withId("UUID")
                .withSource(new URI("http://localhost"))
                .withType("com.example.demo")
                .withData(testData.getBytes())
                .end();
    }


}
