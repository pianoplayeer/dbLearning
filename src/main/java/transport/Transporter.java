package transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * @date 2024/1/21
 * @package transport
 */
public class Transporter {
	private Socket socket;
	private BufferedReader reader;
	private BufferedWriter writer;
	
	public Transporter(Socket socket) throws IOException {
		this.socket = socket;
		this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
	}
	
	public void send(byte[] data) throws Exception {
		String raw = Encoder.hexEncode(data);
		writer.write(raw);
		writer.flush();
	}
	
	public byte[] receive() throws Exception {
		String line = reader.readLine();
		if (line == null) {
			close();
		}
		return Encoder.hexDecode(line);
	}
	
	public void close() throws IOException {
		reader.close();
		writer.close();
		socket.close();
	}
	
	
}
