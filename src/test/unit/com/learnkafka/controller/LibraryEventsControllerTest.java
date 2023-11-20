package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.producer.LibraryEventsProducer;
import com.learnkafka.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = {LibraryEventsController.class})
class LibraryEventsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {

        // ARRANGE
        String jsonLibraryEvent = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        // ACT && ASSERT
        mockMvc
                .perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(jsonLibraryEvent)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.libraryEventId").doesNotExist())
                .andExpect(jsonPath("$.libraryEventType").value("NEW"))
                .andExpect(jsonPath("$.book.bookId").value("123"))
                .andExpect(jsonPath("$.book.bookName").value("Dilip"))
                .andExpect(jsonPath("$.book.bookAuthor").value("Kafka Using Spring Boot"));

    }

    @Test
    void postLibraryEventWithInvalidValues() throws Exception {

        // ARRANGE
        String expectedErrorMessage = "book.bookId - must not be null, book.bookName - must not be blank";
        String jsonLibraryEvent = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());

        // ACT && ASSERT
        mockMvc
                .perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .content(jsonLibraryEvent)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));

    }

}