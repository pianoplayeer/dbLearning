package client;

import transport.Encoder;
import transport.Package;
import transport.Packager;
import transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * @date 2024/1/21
 * @package client
 */
public class Launcher {
	public static void main(String[] args) throws IOException {
		Socket socket = new Socket("127.0.0.1", 9999);
		Encoder encoder = new Encoder();
		Transporter transporter = new Transporter(socket);
		Packager packager = new Packager(transporter, encoder);
		
		Client client = new Client(packager);
		Shell shell = new Shell(client);
		shell.run();
	}
}
