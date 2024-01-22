package backend.dm.pageIndex;

import backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2023/12/5
 * @package backend.dm.pageIndex
 */
public class PageIndex {
	// 最大区间序号
	private static final int INTERVAL_NO = 32;
	private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVAL_NO;
	
	private Lock lock = new ReentrantLock();
	
	// 根据空闲空间大小哈希存储页面信息
	List<PageInfo>[] lists;
	
	@SuppressWarnings("unchecked")
	public PageIndex() {
		lists = new List[INTERVAL_NO + 1];
		for (int i = 0; i < lists.length; i++) {
			lists[i] = new ArrayList<>();
		}
	}
	
	public void add(int pageNo, int freeSpace) {
		lock.lock();
		try {
			int number = freeSpace / THRESHOLD;
			lists[number].add(new PageInfo(pageNo, freeSpace));
		} finally {
			lock.unlock();
		}
	}
	
	public PageInfo select(int spaceSize) {
		lock.lock();
		
		try {
			int number = spaceSize / THRESHOLD;
			if (number < INTERVAL_NO) {
				number++;
			}
			
			while (number <= INTERVAL_NO) {
				if (lists[number].size() == 0) {
					number++;
					continue;
				}
				
				return lists[number].remove(0);
			}
			
			return null;
		} finally {
			lock.unlock();
		}
		
	}
}
