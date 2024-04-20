package transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @date 2024/4/20
 * @package transport
 */
public class EncodeHandler extends MessageToByteEncoder<Package> {
	Encoder encoder = new Encoder();
	
	@Override
	protected void encode(ChannelHandlerContext channelHandlerContext, Package aPackage, ByteBuf byteBuf) throws Exception {
		byteBuf.writeBytes(encoder.encode(aPackage));
	}
}
