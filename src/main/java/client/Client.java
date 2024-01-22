package client;

import transport.Package;
import transport.Packager;

/**
 * @date 2024/1/21
 * @package client
 */
public class Client {
	public RoundTripper roundTripper;
	
	public Client(Packager packager) {
		roundTripper = new RoundTripper(packager);
	}
	
	public byte[] execute(byte[] stat) throws Exception {
		Package pkg = new Package(stat, null);
		pkg = roundTripper.roundTrip(pkg);
		
		if (pkg.getErr() != null) {
			throw pkg.getErr();
		}
		return pkg.getData();
	}
	
	public void close() {
		try {
			roundTripper.close();
		} catch (Exception ignore) {
		}
	}
}
