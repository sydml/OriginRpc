package com.sydml.demo.framework;

import org.apache.commons.lang3.reflect.MethodUtils;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProviderReflect {
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    public static void provider(final Object service, int port) throws Exception {
        ServerSocket serverSocket = new ServerSocket(port);
        executorService.execute(() -> {
            while (true) {
                for (int i = 0; i < 1000; i++) {

                    try (final Socket socket = serverSocket.accept();
                         ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                         ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream())) {
                        String methodName = input.readUTF();
                        Object[] args = (Object[]) input.readObject();
                        Object result = MethodUtils.invokeExactMethod(service, methodName, args);
                        output.writeObject(result);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
}
