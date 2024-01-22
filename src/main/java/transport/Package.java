package transport;

/**
 * @date 2024/1/21
 * @package transport
 */

public class Package {
	private byte[] data;
	private Exception err;
	
	public Package(byte[] data, Exception err) {
		this.data = data;
		this.err = err;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public Exception getErr() {
		return err;
	}
}
