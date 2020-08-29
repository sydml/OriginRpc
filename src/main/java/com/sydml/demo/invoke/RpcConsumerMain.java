package com.sydml.demo.invoke;

import com.sydml.demo.framework.ConsumerProxy;
import com.sydml.demo.service.api.HelloService;

public class RpcConsumerMain {
    public static void main(String[] args) throws Exception {
        HelloService service = ConsumerProxy.consumer(HelloService.class, "127.0.0.1", 8083);
        for (int i = 0; i < 1001; i++) {
            System.out.println(service.sayHello("sydml_" + i));
        }
    }
}
