package backend.dm.dataItem;

/**
 * @date 2023/12/4
 * @package backend.dm.dataItem
 */

import backend.common.SubArray;
import backend.dm.DataManager;
import backend.dm.DataManagerImpl;
import backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 结构：[valid: 1][dataSize: 2][data]
 * valid 0为合法，1为非法
 */
public class DataItemImpl implements DataItem {
	
	public static final int OF_VALID = 0;
	public static final int OF_SIZE = 1;
	public static final int OF_DATA = 3;
	
	private SubArray raw;
	private byte[] oldRaw;
	private Lock rLock;
	private Lock wLock;
	private long uid;
	private Page page;
	private DataManagerImpl dm;
	
	public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dm) {
		this.raw = raw;
		this.oldRaw = oldRaw;
		this.page = page;
		this.uid = uid;
		this.dm = dm;
		ReadWriteLock lock = new ReentrantReadWriteLock();
		rLock = lock.readLock();
		wLock = lock.writeLock();
	}
	
	public boolean isValid() {
		return raw.raw[raw.start + OF_VALID] == 0;
	}
	
	@Override
	public SubArray data() {
		return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
	}
	
	@Override
	public void before() {
		wLock.lock();
		page.setDirty(true);
		System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
	}
	
	@Override
	public void unBefore() {
		System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
		wLock.unlock();
	}
	
	@Override
	public void after(long xid) {
		dm.logDataItem(xid, this);
		wLock.unlock();
	}
	
	@Override
	public void release() {
		dm.releaseDataItem(this);
	}
	
	@Override
	public void lock() {
		wLock.lock();
	}
	
	@Override
	public void unlock() {
		wLock.unlock();
	}
	
	@Override
	public void rLock() {
		rLock.lock();
	}
	
	@Override
	public void rUnlock() {
		rLock.unlock();
	}
	
	@Override
	public Page page() {
		return page;
	}
	
	@Override
	public long getUid() {
		return uid;
	}
	
	@Override
	public byte[] getOldRaw() {
		return oldRaw;
	}
	
	@Override
	public SubArray getRaw() {
		return raw;
	}
}
