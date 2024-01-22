package backend.common;

import common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2023/12/1
 * @package backend.common
 */
public abstract class AbstractCache<T> {
	private HashMap<Long, T> cache = new HashMap<>();
	private HashMap<Long, Integer> references = new HashMap<>();
	private HashMap<Long, Boolean> getting = new HashMap<>();
	
	private int maxResource;
	private int count = 0;
	private Lock lock = new ReentrantLock();
	
	public AbstractCache(int maxResource) {
		this.maxResource = maxResource;
	}
	
	/**
	 * 当资源不在缓存时的获取方式
	 */
	protected abstract T getForCache(long key) throws Exception;
	
	/**
	 * 当资源被驱逐时的写回操作
	 */
	protected abstract void releaseForCache(T obj);
	
	protected T get(long key) throws Exception {
		while (true) {
			lock.lock();
			if (getting.containsKey(key)) {
				lock.unlock();
				
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
				continue;
			}
			
			if (cache.containsKey(key)) {
				T obj = cache.get(key);
				references.put(key, references.get(key) + 1);
				lock.unlock();
				return obj;
			}
			
			// 若已满，则抛出异常
			if (maxResource > 0 && maxResource == count) {
				lock.unlock();
				throw Error.CacheFullException;
			}
			
			count++;
			getting.put(key, true);
			lock.unlock();
			break;
		}
		
		T obj = null;
		try {
			obj = getForCache(key);
		} catch (Exception e) {
			lock.lock();
			count--;
			getting.remove(key);
			lock.unlock();
			throw e;
		}
		
		lock.lock();
		cache.put(key, obj);
		getting.remove(key);
		references.put(key, 1);
		lock.unlock();
		
		return obj;
	}
	
	protected void release(long key) {
		lock.lock();
		try {
			int ref = references.get(key) - 1;
			
			if (ref == 0) {
				T obj = cache.get(key);
				releaseForCache(obj);
				references.remove(key);
				cache.remove(key);
				count--;
			} else {
				references.put(key, ref);
			}
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * 关闭缓存，写回所有资源
	 */
	protected void close() {
		lock.lock();
		try {
			Set<Long> keys = cache.keySet();
			
			for (long k : keys) {
				T obj = cache.get(k);
				releaseForCache(obj);
				references.remove(k);
				cache.remove(k);
			}
		} finally {
			lock.unlock();
		}
	}
}
