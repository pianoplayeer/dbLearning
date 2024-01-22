package backend.dm;

import backend.dm.dataItem.DataItem;
import backend.dm.logger.Logger;
import backend.dm.page.PageOne;
import backend.dm.pageCache.PageCache;
import backend.tm.TransactionManager;

/**
 * @date 2023/12/5
 * @package backend.dm
 */
public interface DataManager {
	DataItem read(long uid) throws Exception;
	long insert(long xid, byte[] data) throws Exception;
	void close();
	
	static DataManager create(String path, long mem, TransactionManager tm) {
		PageCache pc = PageCache.create(path, mem);
		Logger logger = Logger.create(path);
		
		DataManagerImpl dm = new DataManagerImpl(pc, logger, tm);
		dm.initPageOne();
		return dm;
	}
	
	static DataManager open(String path, long mem, TransactionManager tm) {
		PageCache pc = PageCache.open(path, mem);
		Logger logger = Logger.open(path);
		
		DataManagerImpl dm = new DataManagerImpl(pc, logger, tm);
		if (!dm.loadCheckPageOne()) {
			Recover.recover(tm, logger, pc);
		}
		dm.fillPageIndex();
		PageOne.setVcOpen(dm.pageOne);
		dm.pc.flushPage(dm.pageOne);
		
		return dm;
	}
}
