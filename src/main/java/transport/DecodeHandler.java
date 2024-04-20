package transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @date 2024/4/20
 * @package transport
 */
public class DecodeHandler extends ByteToMessageDecoder {
	Encoder encoder = new Encoder();
	
	@Override
	protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
		if (byteBuf.readableBytes() > 0) {
			byte[] info = new byte[byteBuf.readableBytes()];
			byteBuf.readBytes(info);
			list.add(encoder.decode(info));
		}
	}
}
