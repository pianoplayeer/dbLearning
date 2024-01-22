package transport;

import com.google.common.primitives.Bytes;
import common.Error;

import java.util.Arrays;

/**
 * @date 2024/1/21
 * @package transport
 */
public class Encoder {
	/**
	 *
	 * [flag][data]
	 * flag: 0为数据，1为错误
	 */
	public byte[] encode(Package pkg) {
		if (pkg.getErr() != null) {
			Exception err = pkg.getErr();
			String msg = "Internal server error!";
			
			if (err.getMessage() != null) {
				msg = err.getMessage();
			}
			return Bytes.concat(new byte[]{1}, msg.getBytes());
		} else {
			return Bytes.concat(new byte[]{0}, pkg.getData());
		}
	}
	
	public Package decode(byte[] data) throws Exception {
		if (data.length < 1) {
			throw Error.InvalidPkgDataException;
		}
		
		if (data[0] == 0) {
			return new Package(Arrays.copyOfRange(data, 1, data.length), null);
		} else if (data[0] == 1) {
			return new Package(null, new RuntimeException(Arrays.toString(Arrays.copyOfRange(data, 1, data.length))));
		} else {
			throw Error.InvalidPkgDataException;
		}
	}
}
