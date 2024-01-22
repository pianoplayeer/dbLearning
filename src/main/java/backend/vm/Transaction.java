package backend.vm;

import backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @date 2024/1/15
 * @package backend.vm
 */
public class Transaction {
	public long xid;
	public Exception err;
	public Map<Long, Boolean> snapshot;
	public int level;
	public boolean autoAborted;
	
	public static Transaction newTransaction(long xid, int level,
											 Map<Long, Transaction> active) {
		Transaction t = new Transaction();
		t.xid = xid;
		t.level = level;
		
		if (level != 0) {
			t.snapshot = new HashMap<>();
			
			for (long k : active.keySet()) {
				t.snapshot.put(k, true);
			}
		}
		return t;
	}
	
	public boolean isInSnapShot(long xid) {
		if (xid == TransactionManagerImpl.SUPER_XID) {
			return false;
		}
		
		return snapshot.containsKey(xid);
	}
}
