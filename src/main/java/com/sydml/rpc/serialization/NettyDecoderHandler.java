package com.sydml.rpc.serialization;

import com.sydml.rpc.serialization.common.SerializeType;
import com.sydml.rpc.serialization.engine.SerializerEngine;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @author liuyuming
 * @date 2020-08-01 17:29:32
 */
public class NettyDecoderHandler extends ByteToMessageDecoder {
    private Class<?> genericClass;
    private SerializeType serializeType;

    public NettyDecoderHandler(Class<?> genericClass, SerializeType serializeType) {
        this.genericClass = genericClass;
        this.serializeType = serializeType;
    }
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        //获取消息头所标识的消息体字节数组长度
        if (in.readableBytes() < 4) {
            return;
        }
        in.markReaderIndex();
        int dataLength = in.readInt();
        if (dataLength < 0) {
            ctx.close();
        }
        //若当前可以获取到的字节数小于实际长度,则直接返回,直到当前可以获取到的字节数等于实际长度
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }
        //读取完整的消息体字节数组
        byte[] data = new byte[dataLength];
        in.readBytes(data);

        Object obj = SerializerEngine.deserialize(data, genericClass, serializeType.getSerializeType());
        out.add(obj);

    }
}