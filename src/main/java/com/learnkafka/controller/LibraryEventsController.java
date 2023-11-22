package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    @PostMapping(value = "/v1/libraryevent", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("libraryEvent : {}", libraryEvent);
        libraryEventsProducer.sendLibraryEventAsynchronouslyWithProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(libraryEvent);
    }

    @PutMapping(value = "/v1/libraryevent", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("libraryEvent : {}", libraryEvent);
        ResponseEntity<String> validateLibraryEventValue = validateLibraryEvent(libraryEvent);
        if (validateLibraryEventValue != null) return validateLibraryEventValue;
        libraryEventsProducer.sendLibraryEventAsynchronouslyWithProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK)
                .body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Please pass the libraryEventId");
        }
        if (libraryEvent.libraryEventType() != LibraryEventType.UPDATE) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Only UPDATE event type is supported");
        }
        return null;
    }

}
