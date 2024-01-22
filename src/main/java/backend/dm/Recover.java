package backend.dm;

import backend.common.SubArray;
import backend.dm.dataItem.DataItem;
import backend.dm.logger.Logger;
import backend.dm.page.Page;
import backend.dm.page.PageX;
import backend.dm.pageCache.PageCache;
import backend.dm.pageCache.PageCacheImpl;
import backend.tm.TransactionManager;
import backend.utils.Panic;
import backend.utils.Parser;
import com.google.common.primitives.Bytes;
import org.checkerframework.checker.units.qual.A;

import java.util.*;

/**
 * @date 2023/12/3
 * @package backend.dm
 */
public class Recover {
	
	private static final byte LOG_TYPE_INSERT = 0;
	private static final byte LOG_TYPE_UPDATE = 1;
	
	private static final int REDO = 0;
	private static final int UNDO = 1;
	
	
	static class LogInfo {
		long xid;
		int pageNo;
		short offset;
	}
	
	/**
	 * insertLog:
	 * [logType][xid][pageNumber][offset][data]
	 */
	static class InsertLogInfo extends LogInfo {
		byte[] raw;
	}
	
	/**
	 * updateLog:
	 * [logType][xid][uid][oldData][newData]
	 * uid: [pageNo: 4][padding: 2][offset: 2]
	 */
	static class UpdateLogInfo extends LogInfo {
		byte[] oldRaw;
		byte[] newRaw;
	}
	
	public static void recover(TransactionManager tm, Logger logger, PageCache pc) {
		System.out.println("Recovering...");
		
		logger.rewind();
		int maxPageNo = 1;
		byte[] log = null;
		
		while ((log = logger.next()) != null) {
			int pageNo;
			if (isInsertLog(log)) {
				InsertLogInfo logInfo = parseInsertLog(log);
				pageNo = logInfo.pageNo;
			} else {
				UpdateLogInfo logInfo = parseUpdateLog(log);
				pageNo = logInfo.pageNo;
			}
			
			maxPageNo = Math.max(maxPageNo, pageNo);
		}
		
		pc.truncateByBigPageNumber(maxPageNo);
		System.out.println("Truncate to " + maxPageNo + " pages.");
		
		redoTransactions(tm, logger, pc);
		System.out.println("Redo Transactions Over.");
		
		undoTransactions(tm, logger, pc);
		System.out.println("Undo Transactions Over.");
		
		System.out.println("Recover Over.");
	}
	
	private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pc) {
		logger.rewind();
		
		byte[] log;
		while ((log = logger.next()) != null) {
			if (isInsertLog(log)) {
				InsertLogInfo info = parseInsertLog(log);
				
				if (!tm.isActive(info.xid)) {
					doInsertLog(pc, info, REDO);
				}
			} else {
				UpdateLogInfo info = parseUpdateLog(log);
				
				if (!tm.isActive(info.xid)) {
					doUpdateLog(pc, info, REDO);
				}
			}
		}
	}
	
	private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pc) {
		Map<Long, List<LogInfo>> logCache = new HashMap<>();
		logger.rewind();
		byte[] log;
		
		while ((log = logger.next()) != null) {
			LogInfo info;
			long xid;
			
			if (isInsertLog(log)) {
				info = parseInsertLog(log);
			} else {
				info = parseUpdateLog(log);
			}
			xid = info.xid;
			
			if (tm.isActive(xid)) {
				if (!logCache.containsKey(xid)) {
					logCache.put(xid, new ArrayList<>());
				}
				logCache.get(xid).add(info);
			}
		}
		
		for (Map.Entry<Long, List<LogInfo>> e : logCache.entrySet()) {
			List<LogInfo> infos = e.getValue();
			
			// 倒序undo
			for (int i = infos.size() - 1; i >= 0; i--) {
				LogInfo info = infos.get(i);
				
				if (info instanceof InsertLogInfo) {
					doInsertLog(pc, (InsertLogInfo) info, UNDO);
				} else {
					doUpdateLog(pc, (UpdateLogInfo) info, UNDO);
				}
			}
			tm.abort(e.getKey());
		}
	}
	
	private static boolean isInsertLog(byte[] log) {
		return log[0] == LOG_TYPE_INSERT;
	}
	
	
	// [logType: 1][xid: 8][pageNumber: 4][offset: 2][data]
	private static final int OF_TYPE = 0;
	private static final int OF_XID = OF_TYPE + 1;
	private static final int OF_INSERT_PAGE = OF_XID + 8;
	private static final int OF_INSERT_OFFSET = OF_INSERT_PAGE + 4;
	private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;
	
	public static byte[] insertLog(long xid, Page page, byte[] raw) {
		byte[] logType = {LOG_TYPE_INSERT};
		byte[] xidRaw = Parser.long2Byte(xid);
		byte[] pageNo = Parser.int2Byte(page.getPageNumber());
		byte[] offset = Parser.short2Byte(PageX.getFSO(page));
		
		return Bytes.concat(logType, xidRaw, pageNo, offset);
	}
	
	private static InsertLogInfo parseInsertLog(byte[] log) {
		InsertLogInfo logInfo = new InsertLogInfo();
		logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PAGE));
		logInfo.pageNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGE, OF_INSERT_OFFSET));
		logInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
		logInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
		
		return logInfo;
	}
	
	private static void doInsertLog(PageCache pc, InsertLogInfo log, int flag) {
		Page page = null;
		
		try {
			page = pc.getPage(log.pageNo);
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		try {
			if (flag == UNDO) {
				DataItem.setDataItemRawInvalid(log.raw);
			}
			PageX.recoverInsert(page, log.raw, log.offset);
		} finally {
			page.release();
		}
	}
	
	
	// [logType: 1][xid: 8][uid: 8][oldData][newData]
	private static final int OF_UPDATE_UID = OF_XID + 8;
	private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;
	
	public static byte[] updateLog(long xid, DataItem di) {
		byte[] logType = {LOG_TYPE_UPDATE};
		byte[] xidRaw = Parser.long2Byte(xid);
		byte[] uid = Parser.long2Byte(di.getUid());
		byte[] oldRaw = di.getOldRaw();
		SubArray raw = di.getRaw();
		byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
		
		return Bytes.concat(logType, xidRaw, uid, oldRaw, newRaw);
	}
	
	private static UpdateLogInfo parseUpdateLog(byte[] log) {
		UpdateLogInfo logInfo = new UpdateLogInfo();
		logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
		long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
		logInfo.offset = (short) (uid & ((1 << 16) - 1));
		logInfo.pageNo = (int) ((uid >>> 32) & ((1 << 32) - 1));
		
		int len = (log.length - OF_UPDATE_RAW) / 2;
		logInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + len);
		logInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + len, OF_UPDATE_RAW + 2 * len);
		
		return logInfo;
	}
	
	private static void doUpdateLog(PageCache pc, UpdateLogInfo log, int flag) {
		int pageNo = log.pageNo;
		short offset = log.offset;
		byte[] raw;
		
		if (flag == REDO) {
			raw = log.newRaw;
		} else {
			raw = log.oldRaw;
		}
		
		Page page = null;
		try {
			page = pc.getPage(pageNo);
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		try {
			PageX.recoverUpdate(page, raw, offset);
		} finally {
			page.release();
		}
	}
	
	
}
