package com.example.demo.controller;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

@Slf4j
@RestController
public class Controller {

    @Autowired
    KafkaTemplate<String, CloudEvent> kafkaTemplate;

    @GetMapping("/foo")
    public void foo() throws URISyntaxException {

        CloudEvent message = new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSource(new URI("http://localhost"))
                .withType("com.example.demo")
                .withData("{\"foo\": \"foo\"}".getBytes())
                .end();

        ListenableFuture<SendResult<String, CloudEvent>> result = kafkaTemplate.send("foo.topic", message);

        result.addCallback(new KafkaSendCallback<>() {

            @Override
            public void onSuccess(SendResult<String, CloudEvent> stringCloudEventSendResult) {
                log.info(stringCloudEventSendResult.toString());
            }

            @Override
            public void onFailure(KafkaProducerException e) {
                log.error(e.toString());
            }

        });

    }

    @GetMapping("/bar")
    public void bar() throws URISyntaxException {

        CloudEvent message = new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSource(new URI("http://localhost"))
                .withType("com.example.demo")
                .withData("{\"bar\": \"bar\"}".getBytes())
                .end();

        ListenableFuture<SendResult<String, CloudEvent>> result = kafkaTemplate.send("bar.topic", message);

        result.addCallback(new KafkaSendCallback<>() {

            @Override
            public void onSuccess(SendResult<String, CloudEvent> stringCloudEventSendResult) {
                log.info(stringCloudEventSendResult.toString());
            }

            @Override
            public void onFailure(KafkaProducerException e) {
                log.error(e.toString());
            }

        });

    }

}
