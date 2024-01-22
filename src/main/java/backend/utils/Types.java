package backend.utils;

/**
 * @date 2023/12/12
 * @package backend.utils
 */
public class Types {
	public static long addressToUid(int pageNo, short offset) {
		
		return (long) pageNo << 32 | offset;
	}
}
