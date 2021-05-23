package com.example.demo.controller;

import com.example.demo.DemoApplication;
import com.example.demo.model.Bar;
import com.example.demo.model.Foo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.CloudEventUtils;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class ControllerTest {

    @Autowired
    MockMvc mvc;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @SpyBean
    DemoApplication application;

    @Captor
    ArgumentCaptor<CloudEvent> messageCaptor;

    private Foo testFooData;
    private Bar testBarData;

    @BeforeEach
    void setUp() {
        testFooData = getTestFooData();
        testBarData = getTestBarData();
    }

    @Test
    public void testFooEndpoint() throws Exception {

        ContainerTestUtils.waitForAssignment(registry.getListenerContainer("foo-id"), 1);

        mvc.perform(get("http://localhost/foo"));

        Mockito.verify(application, timeout(5000).times(1)).onFooMessage(messageCaptor.capture());

        CloudEvent capturedMessage = messageCaptor.getValue();
        PojoCloudEventData<Foo> mapping = CloudEventUtils.mapData(
                capturedMessage,
                PojoCloudEventDataMapper.from(new ObjectMapper(), Foo.class)
        );

        Foo foo = mapping.getValue();

        assertThat(capturedMessage).isNotNull();
        assertThat(foo.toString()).isEqualTo(testFooData.toString());

    }

    @Test
    public void testBarEndpoint() throws Exception {

        ContainerTestUtils.waitForAssignment(registry.getListenerContainer("bar-id"), 1);

        mvc.perform(get("http://localhost/bar"));

        Mockito.verify(application, timeout(5000).times(1)).onBarMessage(messageCaptor.capture());

        CloudEvent capturedMessage = messageCaptor.getValue();
        PojoCloudEventData<Bar> mapping = CloudEventUtils.mapData(
                capturedMessage,
                PojoCloudEventDataMapper.from(new ObjectMapper(), Bar.class)
        );

        Bar bar = mapping.getValue();

        assertThat(capturedMessage).isNotNull();
        assertThat(bar.toString()).isEqualTo(testBarData.toString());

    }

    private Foo getTestFooData() {
        return new Foo("foo");
    }

    private Bar getTestBarData() {
        return new Bar("bar");
    }

}
