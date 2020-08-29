package com.sydml.rpc.provider;

import com.sydml.rpc.framework.helper.PropertyConfigeHelper;
import com.sydml.rpc.framework.model.RpcRequest;
import com.sydml.rpc.serialization.NettyDecoderHandler;
import com.sydml.rpc.serialization.NettyEncoderHandler;
import com.sydml.rpc.serialization.common.SerializeType;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @author liuyuming
 * @date 2020-08-01 17:05:01
 */
public class NettyServer {
    private static NettyServer nettyServer = new NettyServer();
    private Channel channel;
    //服务端boss线程组
    private EventLoopGroup bossGroup;
    //序列化类型配置信息
    private EventLoopGroup workerGroup;
    //序列化类型配置信息
    private SerializeType serializeType = PropertyConfigeHelper.getSerializeType();

    public void start(final int port) {
        synchronized (NettyServer.class) {
            if (bossGroup != null || workerGroup != null) {
                return;
            }
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            channel.pipeline().addLast(new NettyDecoderHandler(RpcRequest.class, serializeType));
                            channel.pipeline().addLast(new NettyEncoderHandler(serializeType));
                            channel.pipeline().addLast(new NettyServerInvokeHandler());
                        }
                    });
            try {
                channel = serverBootstrap.bind(port).sync().channel();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void stop(){
        if (null == channel) {
            throw new RuntimeException("netty server stopped");
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        channel.closeFuture().syncUninterruptibly();
    }

    private NettyServer(){}

    public static NettyServer singleton(){
        return nettyServer;
    }

}
