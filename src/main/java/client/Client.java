package client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import transport.DecodeHandler;
import transport.EncodeHandler;
import transport.Package;
import transport.Packager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;

/**
 * @date 2024/1/21
 * @package client
 */
public class Client {
	
	private Channel channel;
	private EventLoopGroup eventLoopGroup;
	public static CyclicBarrier barrier = new CyclicBarrier(2);

	public Client(int port) {
		Bootstrap bootstrap = new Bootstrap();
		eventLoopGroup = new NioEventLoopGroup();
		bootstrap.group(eventLoopGroup)
				.channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {
					
					@Override
					protected void initChannel(SocketChannel socketChannel) throws Exception {
						socketChannel
								.pipeline()
								.addLast(new EncodeHandler())
								.addLast(new DecodeHandler())
								.addLast(new ClientHandler());
					}
				});
		
		try {
			channel = bootstrap.connect("127.0.0.1", port).sync().channel();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public byte[] execute(byte[] stat) throws Exception {
		Package pkg = new Package(stat, null);
		Future<Package> future = null;
		
		if (channel.isActive()) {
			channel.writeAndFlush(pkg);
		} else {
			throw new IllegalStateException("Channel is not active.");
		}
		
		barrier.await();
		pkg = (Package) channel.attr(AttributeKey.valueOf("Response")).get();
		
		if (pkg.getErr() != null) {
			throw pkg.getErr();
		}
		return pkg.getData();
	}
	
	public void close() {
		try {
			eventLoopGroup.shutdownGracefully();
		} catch (Exception ignore) {
		}
	}
}

class ClientHandler extends ChannelInboundHandlerAdapter {
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ctx.channel().attr(AttributeKey.valueOf("Response")).set(msg);
		Client.barrier.await();
	}
}
