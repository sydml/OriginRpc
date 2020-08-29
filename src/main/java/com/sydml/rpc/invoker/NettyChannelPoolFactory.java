package com.sydml.rpc.invoker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sydml.rpc.framework.helper.PropertyConfigeHelper;
import com.sydml.rpc.framework.model.ProviderService;
import com.sydml.rpc.framework.model.RpcResponse;
import com.sydml.rpc.serialization.NettyDecoderHandler;
import com.sydml.rpc.serialization.NettyEncoderHandler;
import com.sydml.rpc.serialization.common.SerializeType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * @author liuyuming
 * @date 2020-08-29 14:53:00
 */
public class NettyChannelPoolFactory {
    private static final Logger logger = LoggerFactory.getLogger(NettyChannelPoolFactory.class);
    private static final NettyChannelPoolFactory channelPoolFactory = new NettyChannelPoolFactory();

    private static final Map<InetSocketAddress, ArrayBlockingQueue<Channel>> channelPoolMap = Maps.newConcurrentMap();
    private static final int channelConnectSize = PropertyConfigeHelper.getChannelConnectSize();
    private static final SerializeType serializeType = PropertyConfigeHelper.getSerializeType();
    private List<ProviderService> serviceMetaDataList = Lists.newArrayList();

    private NettyChannelPoolFactory() {
    }
    public static NettyChannelPoolFactory channelPoolFactoryInstance() {
        return channelPoolFactory;
    }

    public void initChannelPoolFactory(Map<String, List<ProviderService>> providerMap) {
        Collection<List<ProviderService>> collectionServiceMetaDataList = providerMap.values();
        for (List<ProviderService> serviceMetaDataModels : collectionServiceMetaDataList) {
            if (CollectionUtils.isEmpty(serviceMetaDataModels)) {
                continue;
            }
            serviceMetaDataList.addAll(serviceMetaDataModels);
        }

        Set<InetSocketAddress> socketAddressSet = Sets.newHashSet();
        for (ProviderService serviceMetaData : serviceMetaDataList) {
            String serverIp = serviceMetaData.getServerIp();
            int serverPort = serviceMetaData.getServerPort();
            InetSocketAddress socketAddress = new InetSocketAddress(serverIp, serverPort);
            socketAddressSet.add(socketAddress);
        }

        for (InetSocketAddress socketAddress : socketAddressSet) {
            try{
                int realChannelConnectSize = 0;
                while (realChannelConnectSize < channelConnectSize) {
                    Channel channel = null;
                    while (channel == null) {
                        channel = registerChannel(socketAddress);
                    }
                    realChannelConnectSize++;
                    ArrayBlockingQueue<Channel> channelArrayBlockingQueue = channelPoolMap.get(socketAddress);
                    if (channelArrayBlockingQueue == null) {
                        channelArrayBlockingQueue = new ArrayBlockingQueue<Channel>(channelConnectSize);
                        channelPoolMap.put(socketAddress, channelArrayBlockingQueue);
                    }
                    channelArrayBlockingQueue.offer(channel);
                }

            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Channel registerChannel(InetSocketAddress socketAddress) {
        try{
            EventLoopGroup group = new NioEventLoopGroup(10);
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.remoteAddress(socketAddress);
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel channel) throws Exception {
                                    channel.pipeline().addLast(new NettyEncoderHandler(serializeType));
                                    channel.pipeline().addLast(new NettyDecoderHandler(RpcResponse.class, serializeType));
                                    channel.pipeline().addLast(new NettyClientInvokeHandler());
                                }
                            }
                    );
            ChannelFuture channelFuture = bootstrap.connect().sync();
            final Channel newChannel = channelFuture.channel();
            final CountDownLatch connectLatch = new CountDownLatch(1);
            final List<Boolean> isSuccessHolder = Lists.newArrayListWithCapacity(1);

            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        isSuccessHolder.add(Boolean.TRUE);
                    } else {
                        future.cause().printStackTrace();
                        isSuccessHolder.add(Boolean.FALSE);
                    }
                    connectLatch.countDown();
                }
            });
            connectLatch.await();
            if (isSuccessHolder.get(0)) {
                return newChannel;
            }

        }catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public ArrayBlockingQueue<Channel> acquire(InetSocketAddress socketAddress) {
        return channelPoolMap.get(socketAddress);
    }

    public void release(ArrayBlockingQueue<Channel> arrayBlockingQueue, Channel channel, InetSocketAddress inetSocketAddress) {
        if (arrayBlockingQueue == null) {
            return;
        }

        //回收之前先检查channel是否可用,不可用的话,重新注册一个,放入阻塞队列
        if (channel == null || !channel.isActive() || !channel.isOpen() || !channel.isWritable()) {
            if (channel != null) {
                channel.deregister().syncUninterruptibly().awaitUninterruptibly();
                channel.closeFuture().syncUninterruptibly().awaitUninterruptibly();
            }
            Channel newChannel = null;
            while (newChannel == null) {
                logger.debug("---------register new Channel-------------");
                newChannel = registerChannel(inetSocketAddress);
            }
            arrayBlockingQueue.offer(newChannel);
            return;
        }
        arrayBlockingQueue.offer(channel);
    }


}
