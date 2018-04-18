package com.sopovs.moradanen.tarantool.spring.boot.session.sample;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;

@RestController
public class HelloRestController {

    @GetMapping("/")
    public String uid(HttpSession session) {
        return session.getId();
    }

}
