package com.sopovs.moradanen.tarantool.spring.boot.test;

import com.sopovs.moradanen.tarantool.spring.boot.session.TarantoolSessionAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(exclude = TarantoolSessionAutoConfiguration.class)
public class TestApplication {

}
