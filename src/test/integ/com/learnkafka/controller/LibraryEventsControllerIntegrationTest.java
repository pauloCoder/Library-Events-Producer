package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;
import jakarta.annotation.Resource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events-test"}, count = 3, partitions = 3, ports = {8085, 8086, 8087})
@TestPropertySource(properties = {
        "spring.kafka.topic=library-events-test",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsControllerIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Resource
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    private Consumer<Integer, String> kafkaConsumer;

    @BeforeEach
    public void setUp() {
        var configs = KafkaTestUtils.consumerProps("console-consumer-1900", "true", embeddedKafkaBroker);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaConsumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(kafkaConsumer);
    }

    @AfterEach
    public void tearDown() {
        kafkaConsumer.close();
    }

    @Test
    void postLibraryEvent() {

        // ARRANGE
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);
        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(), httpHeaders);

        // ACT
        var responseEntity = restTemplate
                .exchange("/v1/libraryevent", HttpMethod.POST, httpEntity, LibraryEvent.class);
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(kafkaConsumer);


        // ASSERT
        Assertions.assertThat(responseEntity)
                .isNotNull()
                .satisfies(response -> Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED));

        Assertions.assertThat(consumerRecords.count()).isEqualTo(1);
        consumerRecords.forEach(record -> {
                    var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
                    Assertions.assertThat(libraryEventActual).isEqualTo(TestUtil.libraryEventRecord());
                }
        );

    }
}