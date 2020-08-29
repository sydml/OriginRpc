package com.sydml.demo.framework;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;

public class ConsumerProxy {
    public static <T> T consumer(final Class<T> interfaceClass, final String host, final int port) throws Exception{
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[]{interfaceClass}, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                try (Socket socket = new Socket(host, port); ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream()); ObjectInputStream input = new ObjectInputStream(socket.getInputStream())) {
                    output.writeUTF(method.getName());
                    output.writeObject(args);
                    Object result = input.readObject();
                    if (result instanceof Throwable) {
                        throw (Throwable) result;
                    }
                    return result;
                }
            }
        });
    }
}
