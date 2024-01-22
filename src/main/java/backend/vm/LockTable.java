package backend.vm;

import common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2024/1/15
 * @package backend.vm
 */
public class LockTable {
	private Map<Long, List<Long>> x2u; // 每个事务所占据的资源
	private Map<Long, Long> u2x; // 每个资源被哪个事务占据
	private Map<Long, List<Long>> wait; // 等待该资源uid的事务xid列表
	private Map<Long, Lock> waitLock; // 正在等待资源的事务的锁
	private Map<Long, Long> waitU; // 事务正在等待的资源
	private Lock lock;
	
	public LockTable() {
		x2u = new HashMap<>();
		u2x = new HashMap<>();
		wait = new HashMap<>();
		waitLock = new HashMap<>();
		waitU = new HashMap<>();
		lock = new ReentrantLock();
	}
	
	private boolean isInList(Map<Long, List<Long>> listMap, long k, long elem) {
		List<Long> l = listMap.get(k);
		
		if (l == null) {
			return false;
		}
		
		for (Long e : l) {
			if (e == elem) {
				return true;
			}
		}
		return false;
	}
	
	private void putIntoList(Map<Long, List<Long>> listMap, long k, long elem) {
		if (!listMap.containsKey(k)) {
			listMap.put(k, new ArrayList<>());
		}
		listMap.get(k).add(0, elem);
	}
	
	private void removeFromList(Map<Long, List<Long>> listMap, long k, long elem) {
		List<Long> l = listMap.get(k);
		
		if (l == null) {
			return;
		}
		
		l.remove(elem);
		if (l.size() == 0) {
			listMap.remove(k);
		}
	}
	
	private Map<Long, Integer> xidStamp;
	private int stamp;
	
	private boolean hasDeadlock() {
		xidStamp = new HashMap<>();
		stamp = 1;
		
		for (long xid : x2u.keySet()) {
			Integer s = xidStamp.get(xid);
			if (s != null && s > 0) {
				continue;
			}
			
			stamp++;
			if (dfs(xid)) {
				return true;
			}
		}
		return false;
	}
	
	private boolean dfs(long xid) {
		Integer s = xidStamp.get(xid);
		if (s != null) {
			if (s == stamp) {
				return true;
			} else if (s < stamp) {
				return false;
			}
		}
		xidStamp.put(xid, stamp);
		
		Long uid = waitU.get(xid);
		if (uid == null) {
			return false;
		}
		Long x = u2x.get(uid);
		assert x != null;
		return dfs(x);
	}
	
	public Lock add(long xid, long uid) throws Exception {
		lock.lock();
		
		try {
			if (isInList(x2u, xid, uid)) {
				return null;
			}
			
			if (!u2x.containsKey(uid)) {
				u2x.put(uid, xid);
				putIntoList(x2u, xid, uid);
				return null;
			}
			
			waitU.put(xid, uid);
			putIntoList(wait, uid, xid);
			
			if (hasDeadlock()) {
				waitU.remove(xid);
				removeFromList(wait, uid, xid);
				throw Error.DeadlockException;
			}
			
			Lock l = new ReentrantLock();
			waitLock.put(xid, l);
			l.lock();
			return l;
		} finally {
			lock.unlock();
		}
	}
	
	public void remove(long xid) {
		lock.lock();
		try {
			List<Long> l = x2u.get(xid);
			if (l != null) {
				while (!l.isEmpty()) {
					selectNewXID(l.remove(0));
				}
			}
			
			waitU.remove(xid);
			x2u.remove(xid);
			waitLock.remove(xid);
		} finally {
			lock.unlock();
		}
	}
	
	private void selectNewXID(long uid) {
		u2x.remove(uid);
		List<Long> l = wait.get(uid);
		
		if (l == null) {
			return;
		}
		
		while (!l.isEmpty()) {
			long xid = l.remove(0);
			
			if (waitLock.containsKey(xid)) {
				u2x.put(uid, xid);
				putIntoList(x2u, xid, uid);
				Lock lock = waitLock.remove(xid);
				waitU.remove(xid);
				lock.unlock();
				break;
			}
		}
		
		if (wait.get(uid).size() == 0) {
			wait.remove(uid);
		}
	}
}
