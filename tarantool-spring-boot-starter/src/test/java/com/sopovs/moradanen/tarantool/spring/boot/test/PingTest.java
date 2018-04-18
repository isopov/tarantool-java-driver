package com.sopovs.moradanen.tarantool.spring.boot.test;

import com.sopovs.moradanen.tarantool.TarantoolTemplate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class PingTest {

    @Autowired
    private TarantoolTemplate tarantoolTemplate;

    @Test
    public void testPing() {
        tarantoolTemplate.ping();
    }

}
