package com.sydml.demo.invoke;

import com.sydml.demo.framework.ProviderReflect;
import com.sydml.demo.service.api.HelloService;
import com.sydml.demo.service.impl.HelloServiceImpl;

public class RpcProviderMain {
    public static void main(String[] args) throws Exception {
        HelloService service = new HelloServiceImpl();
        ProviderReflect.provider(service, 8083);
    }
}
