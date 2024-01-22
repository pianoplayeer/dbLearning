package backend.dm.page;

import backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2023/12/2
 * @package backend.dm.page
 */
public class PageImpl implements Page {
	private int pageNumber;
	private boolean dirty;
	private byte[] data;
	private Lock lock = new ReentrantLock();
	
	private PageCache pc;
	
	public PageImpl(int pageNumber, byte[] data, PageCache pc) {
		this.pageNumber = pageNumber;
		this.data = data;
		this.pc = pc;
	}
	
	@Override
	public void lock() {
		lock.lock();
	}
	
	@Override
	public void unlock() {
		lock.unlock();
	}
	
	@Override
	public void release() {
		pc.release(this);
	}
	
	@Override
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	@Override
	public boolean isDirty() {
		return dirty;
	}
	
	@Override
	public int getPageNumber() {
		return pageNumber;
	}
	
	@Override
	public byte[] getData() {
		return data;
	}
}
