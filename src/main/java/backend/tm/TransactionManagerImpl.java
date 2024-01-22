package backend.tm;


import backend.utils.Panic;
import backend.utils.Parser;
import common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2023/11/29
 * @package tm
 */
public class TransactionManagerImpl implements TransactionManager {
	
	// XID文件头长度
	static final int XID_HEADER_LENGTH = 8;
	
	// 每个事务的占用长度
	private static final int XID_FIELD_SIZE = 1;
	
	// 三种状态
	private static final byte FIELD_TRAN_ACTIVE = 0;
	private static final byte FIELD_TRAN_COMMITTED = 1;
	private static final byte FIELD_TRAN_ABORTED = 2;
	
	// 超级事务，永远为committed状态
	public static final long SUPER_XID = 0;
	
	// XID文件后缀
	static final String XID_SUFFIX = ".xid";
	
	private RandomAccessFile file;
	private FileChannel fc;
	private long xidCounter;
	private Lock counterLock;
	
	public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
		this.file = raf;
		this.fc = fc;
		counterLock = new ReentrantLock();
		checkXIDCounter();
	}
	
	/**
	 * 检查文件头8字节数字是否与文件实际长度相同
	 */
	private void checkXIDCounter() {
		long fileLen = 0;
		
		try {
			fileLen = file.length();
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		if (fileLen < XID_HEADER_LENGTH) {
			 Panic.panic(Error.BadXIDFileException);
		}
		
		ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
		try {
			fc.position(0);
			fc.read(buf);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		xidCounter = Parser.parseLong(buf.array());
		long end = getXidPosition(xidCounter + 1);
		
		if (end != fileLen) {
			Panic.panic(Error.BadXIDFileException);
		}
	}
	
	private long getXidPosition(long xid) {
		return XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
	}
	
	// 检测XID事务是否处于status
	private boolean checkXID(long xid, byte status) {
		ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
		
		try {
			fc.position(getXidPosition(xid));
			fc.read(buf);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		return buf.array()[0] == status;
	}
	
	@Override
	public long begin() {
		counterLock.lock();
		
		long xid = xidCounter + 1;
		updateXID(xid, FIELD_TRAN_ACTIVE);
		incrXIDCounter();
		
		counterLock.unlock();
		return xid;
	}
	
	@Override
	public void commit(long xid) {
		updateXID(xid, FIELD_TRAN_COMMITTED);
	}
	
	@Override
	public void abort(long xid) {
		updateXID(xid, FIELD_TRAN_ABORTED);
	}
	
	@Override
	public boolean isActive(long xid) {
		if (xid == SUPER_XID) {
			return false;
		}
		
		return checkXID(xid, FIELD_TRAN_ACTIVE);
	}
	
	@Override
	public boolean isCommitted(long xid) {
		if (xid == SUPER_XID) {
			return true;
		}
		
		return checkXID(xid, FIELD_TRAN_COMMITTED);
	}
	
	@Override
	public boolean isAborted(long xid) {
		if (xid == FIELD_TRAN_ABORTED) {
			return false;
		}
		
		return checkXID(xid, FIELD_TRAN_ABORTED);
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
	
	// 更新xid事务状态
	private void updateXID(long xid, byte status) {
		long offset = getXidPosition(xid);
		byte[] tmp = new byte[XID_FIELD_SIZE];
		tmp[0] = status;
		
		try {
			fc.position(offset);
			fc.write(ByteBuffer.wrap(tmp));
		} catch (IOException e) {
			Panic.panic(e);
		}
	}
	
	// XID加1，并更新到文件中
	private void incrXIDCounter() {
		xidCounter++;
		
		try {
			fc.position(0);
			fc.write(ByteBuffer.wrap(Parser.long2Byte(xidCounter)));
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		try {
			fc.force(false);
		} catch (IOException e) {
			Panic.panic(e);
		}
	}
}
