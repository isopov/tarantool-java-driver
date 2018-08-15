package com.sopovs.moradanen.tarantool.spring.boot.test;

import com.sopovs.moradanen.tarantool.TarantoolTemplate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class PingTest {

    @Autowired
    private TarantoolTemplate tarantoolTemplate;

    @Test
    public void testPing() {
        tarantoolTemplate.ping();
    }

}
