package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventsProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    @PostMapping(value = "/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("libraryEvent : {}", libraryEvent);
        libraryEventsProducer.sendLibraryEventAsynchronouslyWithProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(libraryEvent);
    }

}
