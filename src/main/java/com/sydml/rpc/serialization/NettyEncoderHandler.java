package com.sydml.rpc.serialization;

import com.sydml.rpc.serialization.common.SerializeType;
import com.sydml.rpc.serialization.engine.SerializerEngine;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author liuyuming
 * @date 2020-08-01 17:29:52
 */
public class NettyEncoderHandler extends MessageToByteEncoder {
    //序列化类型
    private SerializeType serializeType;

    public NettyEncoderHandler(SerializeType serializeType) {
        this.serializeType = serializeType;
    }


    @Override
    protected void encode(ChannelHandlerContext ctx, Object in, ByteBuf out) throws Exception {
        byte[] data = SerializerEngine.serialize(in, serializeType.getSerializeType());
        out.writeInt(data.length);
        out.writeBytes(data);
    }
}
