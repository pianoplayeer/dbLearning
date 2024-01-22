package backend.tm;

import backend.utils.Panic;
import common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @date 2023/11/29
 * @package tm
 */

public interface TransactionManager {
	long begin();
	void commit(long xid);
	void abort(long xid); 			// 取消事务（回滚）
	boolean isActive(long xid);
	boolean isCommitted(long xid);
	boolean isAborted(long xid);
	void close(); 					// 关闭TM
	
	static TransactionManagerImpl create(String path) {
		File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
		
		try {
			if (!file.createNewFile()) {
				Panic.panic(Error.FileExistsException);
			}
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		if (!file.canRead() || !file.canWrite()) {
			Panic.panic(Error.FileCannotRWException);
		}
		
		FileChannel fc = null;
		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(file, "rw");
			fc = raf.getChannel();
		} catch (FileNotFoundException e) {
			Panic.panic(e);
		}
		
		// 清空XID文件头
		try {
			fc.position(0);
			fc.write(ByteBuffer.wrap(new byte[TransactionManagerImpl.XID_HEADER_LENGTH]));
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		return new TransactionManagerImpl(raf, fc);
	}
	
	static TransactionManagerImpl open(String path) {
		File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
		
		if (!file.exists()) {
			Panic.panic(Error.FileNotExistsException);
		}
		
		if (!file.canRead() || !file.canWrite()) {
			Panic.panic(Error.FileCannotRWException);
		}
		
		FileChannel fc = null;
		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(file, "rw");
			fc = raf.getChannel();
		} catch (FileNotFoundException e) {
			Panic.panic(e);
		}
		
		return new TransactionManagerImpl(raf, fc);
	}
}

