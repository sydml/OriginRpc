package com.sydml.demo.service.impl;

import com.sydml.demo.service.api.HelloService;

public class HelloServiceImpl implements HelloService {
    public String sayHello(String content) {
        return "hello," + content;
    }
}
