package backend.server;

import backend.tbm.TableManager;
import lombok.AllArgsConstructor;
import transport.Encoder;
import transport.Package;
import transport.Packager;
import transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
	
	public void start() {
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		System.out.println("Server listen to port: " + port);
		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 20, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
		
		try {
			while (true) {
				Socket s = socket.accept();
				Runnable worker = new HandleSocket(s, tbm);
				threadPoolExecutor.execute(worker);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (Exception ignore) {
			}
		}
	}
}

@AllArgsConstructor
class HandleSocket implements Runnable {
	private Socket socket;
	private TableManager tbm;
	
	
	@Override
	public void run() {
		InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
		System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());
		Packager packager = null;
		
		try {
			Transporter t = new Transporter(socket);
			Encoder e = new Encoder();
			packager = new Packager(t, e);
		} catch (IOException e) {
			e.printStackTrace();
			try {
				socket.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			return;
		}
		
		Executor executor = new Executor(tbm);
		while (true) {
			Package pkg = null;
			try {
				pkg = packager.receive();
			} catch (Exception e) {
				break;
			}
			
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
			try {
				packager.send(pkg);
			} catch (Exception e1) {
				e1.printStackTrace();
				break;
			}
		}
		
		executor.close();
	}
}