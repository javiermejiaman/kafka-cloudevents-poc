package com.example.demo;

import com.example.demo.model.Bar;
import com.example.demo.model.Foo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.CloudEventUtils;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@KafkaListener(id = "foo-id", topics = {"foo.topic"}, groupId = "foo-group")
	public void onFooMessage(CloudEvent message) {

		PojoCloudEventData<Foo> mapping = CloudEventUtils.mapData(
				message,
				PojoCloudEventDataMapper.from(new ObjectMapper(), Foo.class)
		);

		Foo foo = mapping.getValue();

		log.info("El resultado foo es: {}", foo.getFoo());

	}

	@KafkaListener(id = "bar-id", topics = {"bar.topic"}, groupId = "bar-group")
	public void onBarMessage(CloudEvent message) {

		PojoCloudEventData<Bar> mapping = CloudEventUtils.mapData(
				message,
				PojoCloudEventDataMapper.from(new ObjectMapper(), Bar.class)
		);

		Bar bar = mapping.getValue();

		log.info("El resultado bar es: {}", bar.getBar());

	}

}
