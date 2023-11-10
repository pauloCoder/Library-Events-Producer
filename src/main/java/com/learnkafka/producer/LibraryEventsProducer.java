package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topicName;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public void sendLibraryEventAsynchronously(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        // 1. blocking call - get metadata about the kafka cluster
        // 2. Send message happens - Returns a CompleatableFutre
        var completableFuture = kafkaTemplate.send(topicName, key, value);
        completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(throwable);
                    }
                    handleSuccess(key, value, sendResult);
                });
    }

    public void sendLibraryEventSynchronously(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        // 1. blocking call - get metadata about the kafka cluster
        // 2. Block and wait util the message is sent to the Kafka cluster
        var sendResult = kafkaTemplate.send(topicName, key, value)
                        .get(3, TimeUnit.SECONDS);
        handleSuccess(key, value, sendResult);
    }

    public void sendLibraryEventAsynchronouslyWithProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value);
        // 1. blocking call - get metadata about the kafka cluster
        // 2. Send message happens - Returns a CompleatableFutre
        var completableFuture = kafkaTemplate.send(producerRecord);
        completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(throwable);
                    }
                    handleSuccess(key, value, sendResult);
                });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully for the key : {}, the value {} and partition is {}",
                key, value, sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Throwable exception) {
        log.error("Error sending the message and the exception is {} ", exception.getMessage(), exception);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        return new ProducerRecord<>(topicName, key, value);
    }

}
