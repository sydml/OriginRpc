package com.sydml.rpc.invoker;

import com.sydml.rpc.framework.model.RpcRequest;
import com.sydml.rpc.framework.model.RpcResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *  Netty 请求发起线程
 * @author liuyuming
 * @date 2020-08-29 15:25:51
 */
public class RevokerServiceCallable implements Callable<RpcResponse> {
    private static final Logger logger = LoggerFactory.getLogger(RevokerServiceCallable.class);
    private Channel channel;
    private InetSocketAddress inetSocketAddress;
    private RpcRequest request;
    public static RevokerServiceCallable of(InetSocketAddress inetSocketAddress, RpcRequest request) {
        return new RevokerServiceCallable(inetSocketAddress, request);
    }
    public RevokerServiceCallable(InetSocketAddress inetSocketAddress, RpcRequest request) {
        this.inetSocketAddress = inetSocketAddress;
        this.request = request;
    }

    @Override
    public RpcResponse call() throws Exception {
        RevokerResponseHolder.initResponseData(request.getUniqueKey());
        ArrayBlockingQueue<Channel> blockingQueue = NettyChannelPoolFactory.channelPoolFactoryInstance().acquire(inetSocketAddress);
        try{
            if (channel == null) {
                channel = blockingQueue.poll(request.getInvokeTimeout(), TimeUnit.MILLISECONDS);
            }
            while (!channel.isOpen() || !channel.isActive() || !channel.isWritable()) {
                logger.warn("----------retry get new Channel------------");
                channel = blockingQueue.poll(request.getInvokeTimeout(), TimeUnit.MILLISECONDS);
                if (channel == null) {
                    channel = NettyChannelPoolFactory.channelPoolFactoryInstance().registerChannel(inetSocketAddress);
                }
            }
            ChannelFuture channelFuture = channel.writeAndFlush(request);
            channelFuture.syncUninterruptibly();
            long invokeTimeout = request.getInvokeTimeout();
            return RevokerResponseHolder.getValue(request.getUniqueKey(), invokeTimeout);
        }catch (Exception e) {
            logger.error("service invoke error.", e);
        } finally {
            //本次调用完毕后,将Netty的通道channel重新释放到队列中,以便下次调用复用
            NettyChannelPoolFactory.channelPoolFactoryInstance().release(blockingQueue, channel, inetSocketAddress);
        }
        return null;
    }
}
