package backend.vm;

import backend.common.AbstractCache;
import backend.dm.DataManager;
import backend.tm.TransactionManager;
import backend.tm.TransactionManagerImpl;
import backend.utils.Panic;
import common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2023/12/17
 * @package backend.vm
 */
public class VersionManagerImpl extends AbstractCache<Entry>
		implements VersionManager {
	TransactionManager tm;
	DataManager dm;
	Map<Long, Transaction> activeTransaction;
	Lock lock;
	LockTable lockTable;
	
	public VersionManagerImpl(TransactionManager tm, DataManager dm) {
		super(0);
		this.tm = tm;
		this.dm = dm;
		activeTransaction = new HashMap<>();
		activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
		lock = new ReentrantLock();
		lockTable = new LockTable();
	}
	
	
	@Override
	protected Entry getForCache(long uid) throws Exception {
		Entry e = Entry.loadEntry(this, uid);
		
		if (e == null) {
			throw Error.NoEntryException;
		}
		return e;
	}
	
	@Override
	protected void releaseForCache(Entry obj) {
		obj.remove();
	}
	
	
	@Override
	public byte[] read(long xid, long uid) throws Exception {
		lock.lock();
		Transaction t = activeTransaction.get(xid);
		lock.unlock();
		
		assert t.err == null : t.err;
		
		Entry e = null;
		try {
			e = get(uid);
		} catch (Exception ex) {
			if (ex == Error.NoEntryException) {
				return null;
			} else {
				throw ex;
			}
		}
		
		try {
			if (Visibility.isVisible(tm, t, e)) {
				return e.data();
			} else {
				return null;
			}
		} finally {
			e.release();
		}
	}
	
	@Override
	public long insert(long xid, byte[] data) throws Exception {
		lock.lock();
		Transaction t = activeTransaction.get(xid);
		lock.unlock();
		
		assert t.err == null : t.err;
		
		byte[] raw = Entry.wrapEntry(data, xid);
		return dm.insert(xid, raw);
	}
	
	@Override
	public boolean delete(long xid, long uid) throws Exception {
		lock.lock();
		Transaction t = activeTransaction.get(xid);
		lock.unlock();
		
		assert t.err == null : t.err;
		
		Entry entry = null;
		try {
			entry = get(uid);
		} catch (Exception e) {
			if (e == Error.NoEntryException) {
				return false;
			} else {
				throw e;
			}
		}
		
		try {
			if (!Visibility.isVisible(tm, t, entry)) {
				return false;
			}
			
			Lock l = null;
			try {
				l = lockTable.add(xid, uid);
			} catch (Exception e) {
				t.err = Error.ConcurrentUpdateException;
				internAbort(xid, true);
				t.autoAborted = true;
				throw t.err;
			}
			
			if (l != null) {
				l.lock();
				l.unlock();
			}
			
			if (entry.getXmax() == xid) {
				return false;
			}
			
			if (Visibility.isVersionSkip(tm, t, entry)) {
				t.err = Error.ConcurrentUpdateException;
				internAbort(xid, true);
				t.autoAborted = true;
				throw t.err;
			}
			
			entry.setXmax(xid);
			return true;
		} finally {
			entry.release();
		}
	}
	
	@Override
	public long begin(int level) {
		lock.lock();
		try {
			long xid = tm.begin();
			Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
			activeTransaction.put(xid, t);
			return xid;
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public void commit(long xid) throws Exception {
		lock.lock();
		Transaction t = activeTransaction.get(xid);
		lock.unlock();
		
		try {
			if (t.err != null) {
				throw t.err;
			}
		} catch (NullPointerException e) {
			System.out.println(xid);
			System.out.println(activeTransaction.keySet());
			Panic.panic(e);
		}
		
		lock.lock();
		activeTransaction.remove(xid);
		lock.unlock();
		
		lockTable.remove(xid);
		tm.commit(xid);
	}
	
	@Override
	public void abort(long xid) {
		internAbort(xid, false);
	}
	
	private void internAbort(long xid, boolean autoAborted) {
		lock.lock();
		Transaction t = activeTransaction.get(xid);
		if (!autoAborted) {
			activeTransaction.remove(xid);
		}
		lock.unlock();
		
		if (t.autoAborted) {
			return;
		}
		
		lockTable.remove(xid);
		tm.abort(xid);
	}
	
	public void releaseEntry(Entry entry) {
		release(entry.getUid());
	}
}
