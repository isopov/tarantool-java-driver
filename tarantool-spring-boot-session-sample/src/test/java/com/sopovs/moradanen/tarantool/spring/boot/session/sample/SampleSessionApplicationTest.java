package com.sopovs.moradanen.tarantool.spring.boot.session.sample;

import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.MockMvcPrint;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_ATTRIBUTES_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.test.TestUtil.getEnvTarantoolVersion;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@AutoConfigureMockMvc(print = MockMvcPrint.NONE)
class SampleSessionApplicationTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private TarantoolClientSource clientSource;
    @Autowired
    private TarantoolSessionRepository repo;

    @BeforeEach
    void setup() {
        assumeFalse(getEnvTarantoolVersion().startsWith("1.6"));
        repo.createSpaces();
    }

    @AfterEach
    void tearDown() {
        if (!getEnvTarantoolVersion().startsWith("1.6")) {
            try (TarantoolClient client = clientSource.getClient()) {
                client.evalFully("box.space." + DEFAULT_SPACE_NAME + ":drop()").consume();
                client.evalFully("box.space." + DEFAULT_ATTRIBUTES_SPACE_NAME + ":drop()").consume();
            }
        }
    }

    @Test
    void testGet() throws Exception {
        String sessionId = mockMvc.perform(get("/"))
                .andReturn().getResponse().getContentAsString();

        assertNotNull(repo.findById(sessionId));
    }
}
