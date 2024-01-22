package client;

import lombok.AllArgsConstructor;
import transport.Package;
import transport.Packager;

/**
 * @date 2024/1/21
 * @package client
 */

@AllArgsConstructor
public class RoundTripper {
	private Packager packager;
	
	public Package roundTrip(Package pkg) throws Exception {
		packager.send(pkg);
		return packager.receive();
	}
	
	public void close() throws Exception {
		packager.close();
	}
}
