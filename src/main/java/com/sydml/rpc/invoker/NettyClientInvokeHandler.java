package com.sydml.rpc.invoker;

import com.sydml.rpc.framework.model.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author liuyuming
 * @date 2020-08-29 15:14:02
 */
public class NettyClientInvokeHandler extends SimpleChannelInboundHandler<RpcResponse> {
    public NettyClientInvokeHandler() {
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcResponse rpcResponse) throws Exception {
        //将Netty异步返回的结果存入阻塞队列,以便调用端同步获取
        RevokerResponseHolder.putResultValue(rpcResponse);
    }
}
