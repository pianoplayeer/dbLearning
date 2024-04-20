package backend.utils;

import com.google.common.primitives.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @date 2023/11/29
 * @package utils
 */
public class Parser {
	
	public static int parseInt(byte[] buf) {
		return ByteBuffer.wrap(buf, 0, 4).getInt();
	}
	
	public static long parseLong(byte[] buf) {
		return ByteBuffer.wrap(buf, 0, 8).getLong();
	}
	
	public static byte[] long2Byte(long val) {
		return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(val).array();
	}
	
	public static byte[] short2Byte(short val) {
		return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(val).array();
	}
	
	public static byte[] int2Byte(int val) {
		return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(val).array();
	}
	
	public static short parseShort(byte[] buf) {
		return ByteBuffer.wrap(buf, 0, 2).getShort();
	}
	
	public static ParseStringRes parseString(byte[] raw) {
		int len = parseInt(Arrays.copyOf(raw, 4));
		String str = new String(Arrays.copyOfRange(raw, 4, 4 + len));
		return new ParseStringRes(str, 4 + len);
	}
	
	public static byte[] string2Byte(String str) {
		byte[] size = int2Byte(str.length());
		return Bytes.concat(size, str.getBytes());
	}
	
	public static long str2Uid(String str) {
		long seed = 13331;
		long res = 0;
		
		for (byte b : str.getBytes()) {
			res += res * seed + b;
		}
		return res;
	}
}
