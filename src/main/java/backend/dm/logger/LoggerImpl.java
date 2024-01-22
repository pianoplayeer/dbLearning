package backend.dm.logger;

/**
 * @date 2023/12/3
 * @package backend.dm.logger
 */

import backend.utils.Panic;
import backend.utils.Parser;
import com.google.common.primitives.Bytes;
import common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 * 日志标准格式：
 * [XCheckSum][log1][log2]...[logn]([BadTail])
 * XCheckSum int
 *
 * 每条日志格式：
 * [size][checkSum][data]
 * size标识data长度
 * checkSum 4字节int
 */
public class LoggerImpl implements Logger {
	
	private static final int SEED = 13331;
	
	private static final int OF_SIZE = 0;
	private static final int OF_CHECKSUM = OF_SIZE + 4;
	private static final int OF_DATA = OF_CHECKSUM + 4;
	
	public static final String LOG_SUFFIX = ".log";
	
	private RandomAccessFile file;
	private FileChannel fc;
	private Lock lock = new ReentrantLock();
	
	// 当前日志指针的位置
	private long position;
	
	// 初始化时记录，log操作不会更新
	private long fileSize;
	
	private int xCheckSum;
	
	public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
		file = raf;
		this.fc = fc;
	}
	
	public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
		this(raf, fc);
		this.xCheckSum = xCheckSum;
	}
	
	void init() {
		long size = 0;
		try {
			size = file.length();
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		// 大小连XCheckSum都不够
		if (size < 4) {
			Panic.panic(Error.BadLogFileException);
		}
		
		ByteBuffer buf = ByteBuffer.allocate(4);
		try {
			fc.position(0);
			fc.read(buf);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		fileSize = size;
		xCheckSum = Parser.parseInt(buf.array());
		checkAndRemoveTail();
	}
	
	/**
	 * 检查并移除bad tail
	 */
	private void checkAndRemoveTail() {
		rewind();
		
		int check = 0;
		byte[] log;
		
		while ((log = internNext()) != null) {
			check = calCheckSum(check, log);
		}
		
		if (check != xCheckSum) {
			Panic.panic(Error.BadLogFileException);
		}
		
		try {
			truncate(position);
			file.seek(position);
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		rewind();
	}
	
	private void updateXCheckSum(byte[] log) {
		xCheckSum = calCheckSum(xCheckSum, log);
		try {
			fc.position(0);
			fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
			fc.force(false);
		} catch (IOException e) {
			Panic.panic(e);
		}
	}
	
	private byte[] wrapLog(byte[] data) {
		byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
		byte[] size = Parser.int2Byte(data.length);
		return Bytes.concat(size, checkSum, data);
	}
	
	@Override
	public void log(byte[] data) {
		byte[] log = wrapLog(data);
		ByteBuffer buf = ByteBuffer.wrap(log);
		
		lock.lock();
		try {
			fc.position(fc.size());
			fc.write(buf);
		} catch (IOException e) {
			Panic.panic(e);
		} finally {
			lock.unlock();
		}
		
		updateXCheckSum(log);
	}
	
	@Override
	public void truncate(long x) throws Exception {
		lock.lock();
		try {
			fc.truncate(x);
		} finally {
			lock.unlock();
		}
	}
	
	private int calCheckSum(int xCheck, byte[] log) {
		for (byte b : log) {
			xCheck = xCheck * SEED + b;
		}
		
		return xCheck;
	}
	
	private byte[] internNext() {
		if (position + OF_DATA >= fileSize) {
			return null;
		}
		
		ByteBuffer tmp = ByteBuffer.allocate(4);
		try {
			fc.position(position);
			fc.read(tmp);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		int size = Parser.parseInt(tmp.array());
		if (position + OF_DATA + size >= fileSize) {
			return null;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
		try {
			fc.position(position);
			fc.read(buf);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		byte[] log = buf.array();
		int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
		int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
		
		if (checkSum1 != checkSum2) {
			return null;
		}
		
		position += log.length;
		return log;
	}
	
	@Override
	public byte[] next() {
		lock.lock();
		try {
			byte[] log = internNext();
			if (log == null) {
				return null;
			}
			return Arrays.copyOfRange(log, OF_DATA, log.length);
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public void rewind() {
		position = 4;
	}
	
	@Override
	public void close() {
		try {
			fc.close();
			file.close();
		} catch (IOException e) {
			Panic.panic(e);
		}
	}
}
