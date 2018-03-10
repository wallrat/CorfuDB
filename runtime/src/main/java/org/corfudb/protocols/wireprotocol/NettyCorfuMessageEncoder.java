package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<CorfuMsg> {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          CorfuMsg corfuMsg,
                          ByteBuf byteBuf) throws Exception {
        try {
            corfuMsg.serialize(byteBuf);
        } catch (Exception e) {
            log.error("encode: Error during serialization!", e);
        }
    }
}
