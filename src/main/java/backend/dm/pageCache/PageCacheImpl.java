package backend.dm.pageCache;

import backend.common.AbstractCache;
import backend.dm.page.Page;
import backend.dm.page.PageImpl;
import backend.utils.Panic;
import common.Error;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2023/12/2
 * @package backend.dm.pageCache
 */
public class PageCacheImpl extends AbstractCache<Page>
							implements PageCache {
	
	private static final int MEM_MIN_LIM = 10;
	public static final String DB_SUFFIX = ".db";
	
	private RandomAccessFile file;
	private FileChannel fc;
	private Lock fileLock;
	
	// 页数目
	private AtomicInteger pageNumbers;
	
	public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResources) {
		super(maxResources);
		if (maxResources < MEM_MIN_LIM) {
			Panic.panic(Error.MemTooSmallException);
		}
		
		long length = 0;
		try {
			length = file.length();
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		this.file = file;
		this.fc = fc;
		this.fileLock = new ReentrantLock();
		pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
	}
	
	
	@Override
	protected Page getForCache(long key) throws Exception {
		int pageNo = (int) key;
		long offset = pageOffset(pageNo);
		
		ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
		
		fileLock.lock();
		try {
			fc.position(offset)
					.read(buf);
		} catch (IOException e) {
			Panic.panic(e);
		}
		fileLock.unlock();
		
		return new PageImpl(pageNo, buf.array(), this);
	}
	
	@Override
	protected void releaseForCache(Page obj) {
		if (obj.isDirty()) {
			flush(obj);
			obj.setDirty(false);
		}
	}
	
	private long pageOffset(int pageNo) {
		return (long) (pageNo - 1) * PAGE_SIZE;
	}
	
	private void flush(Page page) {
		int pageNo = page.getPageNumber();
		long offset = pageOffset(pageNo);
		
		fileLock.lock();
		try {
			ByteBuffer buf = ByteBuffer.wrap(page.getData());
			fc.position(offset)
					.write(buf);
			fc.force(false);
		} catch (IOException e) {
			Panic.panic(e);
		} finally {
			fileLock.unlock();
		}
	}
	
	@Override
	public int newPage(byte[] initData) {
		int pageNo = pageNumbers.incrementAndGet();
		Page page = new PageImpl(pageNo, initData, null);
		flush(page);
		return pageNo;
	}
	
	@Override
	public Page getPage(int pageNo) throws Exception {
		return get(pageNo);
	}
	
	@Override
	public void close() {
		super.close();
		try {
			fc.close();
			file.close();
		} catch (IOException e) {
			Panic.panic(e);
		}
	}
	
	@Override
	public void release(Page page) {
		release(page.getPageNumber());
	}
	
	
	@Override
	public void truncateByBigPageNumber(int maxPageNo) {
		long size = pageOffset(maxPageNo + 1);
		
		try {
			file.setLength(size);
		} catch (IOException e) {
			Panic.panic(e);
		}
		
		pageNumbers.set(maxPageNo);
	}
	
	@Override
	public int getPageNumber() {
		return pageNumbers.intValue();
	}
	
	@Override
	public void flushPage(Page page) {
		flush(page);
	}
}
