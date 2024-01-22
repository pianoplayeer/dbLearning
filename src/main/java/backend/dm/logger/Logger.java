package backend.dm.logger;

import backend.utils.Panic;
import backend.utils.Parser;
import common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @date 2023/12/3
 * @package backend.dm.logger
 */
public interface Logger {
	void log(byte[] data);
	void truncate(long x) throws Exception;
	byte[] next();
	void rewind();
	void close();
	
	static Logger create(String path) {
		File f = new File(path + LoggerImpl.LOG_SUFFIX);
		try {
			if(!f.createNewFile()) {
				Panic.panic(Error.FileExistsException);
			}
		} catch (Exception e) {
			Panic.panic(e);
		}
		if(!f.canRead() || !f.canWrite()) {
			Panic.panic(Error.FileCannotRWException);
		}
		
		FileChannel fc = null;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
		} catch (FileNotFoundException e) {
			Panic.panic(e);
		}
		
		ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
		try {
			fc.position(0);
			fc.write(buf);
			fc.force(false);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		return new LoggerImpl(raf, fc, 0);
	}
	
	static Logger open(String path) {
		File f = new File(path+LoggerImpl.LOG_SUFFIX);
		if(!f.exists()) {
			Panic.panic(Error.FileNotExistsException);
		}
		if(!f.canRead() || !f.canWrite()) {
			Panic.panic(Error.FileCannotRWException);
		}
		
		FileChannel fc = null;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(f, "rw");
			fc = raf.getChannel();
		} catch (FileNotFoundException e) {
			Panic.panic(e);
		}
		
		LoggerImpl lg = new LoggerImpl(raf, fc);
		lg.init();
		
		return lg;
	}
}
