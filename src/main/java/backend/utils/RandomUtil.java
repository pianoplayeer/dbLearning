package backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @date 2023/12/2
 * @package backend.utils
 */
public class RandomUtil {
	
	public static byte[] randomBytes(int length) {
		Random r = new SecureRandom();
		byte[] buf = new byte[length];
		r.nextBytes(buf);
		return buf;
	}
}
