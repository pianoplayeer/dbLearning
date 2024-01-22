package transport;

/**
 * @date 2024/1/21
 * @package transport
 */
public class Packager {
	private Transporter transporter;
	private Encoder encoder;
	
	public Packager(Transporter transporter, Encoder encoder) {
		this.transporter = transporter;
		this.encoder = encoder;
	}
	
	public void send(Package pkg) throws Exception {
		transporter.send(encoder.encode(pkg));
	}
	
	public Package receive() throws Exception {
		return encoder.decode(transporter.receive());
	}
	
	public void close() throws Exception {
		transporter.close();
	}
}
