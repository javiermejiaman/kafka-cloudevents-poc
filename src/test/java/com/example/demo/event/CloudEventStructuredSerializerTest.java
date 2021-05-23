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

public class CloudEventStructuredSerializerTest {

    private CloudEventsStructuredSerializer structuredSerializer;

    private String topic;
    private RecordHeaders headers;
    private CloudEvent message;

    private String testData;
    private String expectedData;
    private Map<String, Object> testConfig;

    @BeforeEach
    void setUp() throws URISyntaxException {
        structuredSerializer = getSerializer();

        testData = getTestData();
        expectedData = getExpectedData();
        testConfig = getTestConfig();

        topic = getTopic();
        headers = getHeaders();
        message = getMessage();
    }

    @AfterEach
    void tearDown() {
        structuredSerializer = null;

        testData = null;
        expectedData = null;
        testConfig = null;

        topic = null;
        headers = null;
        message = null;
    }

    @Test
    void structuredEncodingTest() {

        structuredSerializer.configure(testConfig, false);

        byte[] serialized = structuredSerializer.serialize(topic, headers, message);

        assertThat(new String(serialized, StandardCharsets.UTF_8)).isEqualTo(expectedData);

    }

    @Test
    void assetThrowsKeySerializerExceptionTest() {
        assertThrows(
                IllegalArgumentException.class,
                () -> structuredSerializer.configure(testConfig, true)
        );
    }

    @Test
    void assertNoThrowsValueSerializerTest() {
        assertDoesNotThrow(() -> structuredSerializer.configure(testConfig, false));
    }

    private CloudEventsStructuredSerializer getSerializer() {
        return new CloudEventsStructuredSerializer();
    }

    private String getTestData() {
        return "{\"key\": \"value\"}";
    }

    private String getExpectedData() {
        return "{\"specversion\":\"1.0\",\"id\":\"UUID\",\"source\":\"http://localhost\",\"type\":\"com.example.demo\",\"data\":{\"key\": \"value\"}}";
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
