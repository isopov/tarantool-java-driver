package com.sopovs.moradanen.tarantool.spring.boot.test;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.sopovs.moradanen.tarantool.spring.boot.session.TarantoolSessionAutoConfiguration;

@SpringBootApplication(exclude = TarantoolSessionAutoConfiguration.class)
public class TestApplication {

}
