package com.sopovs.moradanen.tarantool.spring.boot.session.sample;

import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.MockMvcPrint;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_ATTRIBUTES_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.spring.session.TarantoolSessionRepository.DEFAULT_SPACE_NAME;
import static com.sopovs.moradanen.tarantool.test.TestUtil.getEnvTarantoolVersion;
import static org.junit.Assert.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc(print = MockMvcPrint.NONE)
public class SampleSessionApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TarantoolClientSource clientSource;

    @Autowired
    private TarantoolSessionRepository repo;

    @Before
    public void setup() {
        Assume.assumeFalse(getEnvTarantoolVersion().startsWith("1.6"));
        repo.createSpaces();
    }

    @After
    public void tearDown() {
        if (!getEnvTarantoolVersion().startsWith("1.6")) {
            try (TarantoolClient client = clientSource.getClient()) {
                client.evalFully("box.space." + DEFAULT_SPACE_NAME + ":drop()").consume();
                client.evalFully("box.space." + DEFAULT_ATTRIBUTES_SPACE_NAME + ":drop()").consume();
            }
        }
    }

    @Test
    public void testGet() throws Exception {
        String sessionId = mockMvc.perform(get("/"))
                .andReturn().getResponse().getContentAsString();

        assertNotNull(repo.findById(sessionId));

    }
}
