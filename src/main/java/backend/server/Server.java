package backend.server;

import backend.tbm.TableManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.AllArgsConstructor;
import transport.*;
import transport.Package;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @date 2024/1/21
 * @package server
 */
public class Server {
	private int port;
	private TableManager tbm;
	
	public Server(int port, TableManager tbm) {
		this.port = port;
		this.tbm = tbm;
	}
	
//	public void start() {
//		ServerSocket socket = null;
//		try {
//			socket = new ServerSocket(port);
//		} catch (IOException e) {
//			e.printStackTrace();
//			return;
//		}
//
//		System.out.println("Server listen to port: " + port);
//		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 20, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
//
//		try {
//			while (true) {
//				Socket s = socket.accept();
//				Runnable worker = new HandleSocket(s, tbm);
//				threadPoolExecutor.execute(worker);
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				socket.close();
//			} catch (Exception ignore) {
//			}
//		}
//	}
	public void start() {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		
		try {
			ServerBootstrap bootstrap = new ServerBootstrap();
			bootstrap.group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class)
					.option(ChannelOption.SO_BACKLOG, 128)
					.childOption(ChannelOption.SO_KEEPALIVE, true)
					.childHandler(new ChannelInitializer<SocketChannel>() {
						
						@Override
						protected void initChannel(SocketChannel socketChannel) {
							socketChannel.pipeline().addLast(new EncodeHandler())
													.addLast(new DecodeHandler())
													.addLast(new HandleSocket(tbm));
						}
					});
			
			ChannelFuture closeFuture = bootstrap.bind(port).sync();
			closeFuture.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
		
	}
}

class HandleSocket extends ChannelInboundHandlerAdapter {
	private TableManager tbm;
	private Encoder encoder;
	private Executor executor;
	
	public HandleSocket(TableManager tbm) {
		this.tbm = tbm;
		executor = new Executor(tbm);
	}
	
//	public void run() {
//		InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
//		System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());
//		Packager packager = null;
//
//		try {
//			Transporter t = new Transporter(socket);
//			Encoder e = new Encoder();
//			packager = new Packager(t, e);
//		} catch (IOException e) {
//			e.printStackTrace();
//			try {
//				socket.close();
//			} catch (IOException e1) {
//				e1.printStackTrace();
//			}
//			return;
//		}
//
//		Executor executor = new Executor(tbm);
//		while (true) {
//			Package pkg = null;
//			try {
//				pkg = packager.receive();
//			} catch (Exception e) {
//				break;
//			}
//
//			byte[] sql = pkg.getData();
//			byte[] res = null;
//			Exception e = null;
//
//			try {
//				res = executor.execute(sql);
//			} catch (Exception e1) {
//				e = e1;
//				e.printStackTrace();
//			}
//
//			pkg = new Package(res, e);
//			try {
//				packager.send(pkg);
//			} catch (Exception e1) {
//				e1.printStackTrace();
//				break;
//			}
//		}
//
//		executor.close();
//	}
	
	
	@Override
	public void channelRead(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
		Package pkg = (Package) o;
		Executor executor = new Executor(tbm);
		
		byte[] sql = pkg.getData();
		byte[] res = null;
		Exception e = null;
		
		try {
			res = executor.execute(sql);
		} catch (Exception e1) {
			e = e1;
			e.printStackTrace();
		}
		
		pkg = new Package(res, e);
		channelHandlerContext.writeAndFlush(pkg);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext channelHandlerContext) throws Exception {
		executor.close();
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
		executor.close();
		channelHandlerContext.close();
	}
}