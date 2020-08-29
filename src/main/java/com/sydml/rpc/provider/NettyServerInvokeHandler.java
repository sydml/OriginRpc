package com.sydml.rpc.provider;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.sydml.rpc.framework.model.ProviderService;
import com.sydml.rpc.framework.model.RpcRequest;
import com.sydml.rpc.framework.model.RpcResponse;
import com.sydml.rpc.zookeeper.IRegisterCenterProvider;
import com.sydml.rpc.zookeeper.RegisterCenter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * @author liuyuming
 * @date 2020-08-29 14:05:44
 */
@ChannelHandler.Sharable
public class NettyServerInvokeHandler extends SimpleChannelInboundHandler<RpcRequest> {
    private static final Logger logger = LoggerFactory.getLogger(NettyServerInvokeHandler.class);
    private static final Map<String, Semaphore> serviceKeySemaphoreMap = Maps.newConcurrentMap();

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        //发生异常,关闭链路
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) throws Exception {
        if (ctx.channel().isWritable()) {
            ProviderService metaDataModel = request.getProviderService();
            long consumerTimeOut = request.getInvokeTimeout();
            final String methodName = request.getInvokedMethodName();
            String serviceKey = metaDataModel.getServiceInterface().getName();
            int workerThreads = metaDataModel.getWorkerThreads();
            Semaphore semaphore = serviceKeySemaphoreMap.get(serviceKey);
            if (semaphore == null) {
                synchronized (serviceKeySemaphoreMap) {
                    semaphore = serviceKeySemaphoreMap.get(serviceKey);
                    if (semaphore == null) {
                        semaphore = new Semaphore(workerThreads);
                        serviceKeySemaphoreMap.put(serviceKey, semaphore);
                    }
                }
            }

            IRegisterCenterProvider registerCenter = RegisterCenter.singleton();
            List<ProviderService> localProviderCaches = registerCenter.getProviderServiceMap().get(serviceKey);
            Object result = null;
            boolean acquire = false;
            try{
                ProviderService localProviderCache = Collections2.filter(localProviderCaches, providerService -> StringUtils.equals(providerService.getServiceMethod().getName(), methodName)).iterator().next();
                Object serviceObject = localProviderCache.getServiceObject();
                Method method = localProviderCache.getServiceMethod();
                acquire = semaphore.tryAcquire(consumerTimeOut, TimeUnit.MILLISECONDS);
                if (acquire) {
                    result = method.invoke(serviceObject, request.getArgs());
                }
            }catch (Exception e) {
                logger.info(JSON.toJSONString(localProviderCaches) + " " + methodName + " " + e.getMessage());
                result = e;
            } finally {
                if (acquire) {
                    semaphore.release();
                }
            }

            RpcResponse response = new RpcResponse();
            response.setInvokeTimeout(consumerTimeOut);
            response.setUniqueKey(request.getUniqueKey());
            response.setResult(result);
            ctx.writeAndFlush(response);
        }else {
            logger.error("------------channel closed ! -------------------");
        }

    }
}
