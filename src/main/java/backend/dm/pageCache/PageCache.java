package backend.dm.pageCache;

import backend.dm.page.Page;
import backend.dm.page.PageImpl;
import backend.utils.Panic;
import common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


/**
 * @date 2023/12/2
 * @package backend.dm.pageCache
 */
public interface PageCache {
	
	int PAGE_SIZE = 1 << 13;
	
	int newPage(byte[] initData);
	
	Page getPage(int pageNo) throws Exception;
	
	void close();
	
	void release(Page page);
	
	void truncateByBigPageNumber(int maxPageNo);
	
	int getPageNumber();
	
	void flushPage(Page page);
	
	static PageCacheImpl create(String path, long memory) {
		File file = new File(path + PageCacheImpl.DB_SUFFIX);
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
		
		return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
	}
	
	static PageCacheImpl open(String path, long memory) {
		File file = new File(path + PageCacheImpl.DB_SUFFIX);
		
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
		
		return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
		
	}
}