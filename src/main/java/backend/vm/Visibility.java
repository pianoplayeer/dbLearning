package backend.vm;

import backend.tm.TransactionManager;

import java.util.logging.Level;

/**
 * @date 2024/1/14
 * @package backend.vm
 */
public class Visibility {
	
	public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
		long xmax = e.getXmax();
		
		if (t.level == 0) {
			return false;
		} else {
			return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapShot(xmax));
		}
	}
	
	public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
		if (t.level == 0) {
			return readCommitted(tm, t, e);
		} else {
			return repeatableRead(tm, t, e);
		}
	}
	
	private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
		long xmin = e.getXmin();
		long xmax = e.getXmax();
		long xid = t.xid;
		
		if (xmin == xid && xmax == 0) {
			return true;
		}
		
		return tm.isCommitted(xmin) &&
					   (xmax == 0 || (xmax != xid && !tm.isCommitted(xmax)));
	}
	
	private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
		long xmin = e.getXmin();
		long xmax = e.getXmax();
		long xid = t.xid;
		
		if (xmin == xid && xmax == 0) {
			return true;
		}
		
		if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapShot(xmin)) {
			if (xmax == 0) {
				return true;
			}
			
			if (xmax != xid ) {
				return !tm.isCommitted(xmax) && xmax > xid && t.isInSnapShot(xmax);
			}
		}
		
		return false;
	}
}
