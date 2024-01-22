package backend.dm;

import backend.common.AbstractCache;
import backend.dm.dataItem.DataItem;
import backend.dm.dataItem.DataItemImpl;
import backend.dm.logger.Logger;
import backend.dm.page.Page;
import backend.dm.page.PageOne;
import backend.dm.page.PageX;
import backend.dm.pageCache.PageCache;
import backend.dm.pageIndex.PageIndex;
import backend.dm.pageIndex.PageInfo;
import backend.tm.TransactionManager;
import backend.utils.Panic;
import backend.utils.Types;
import common.Error;

/**
 * @date 2023/12/5
 * @package backend.dm
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
	
	TransactionManager tm;
	PageCache pc;
	Logger logger;
	PageIndex pageIndex;
	Page pageOne;
	
	private static final int ATTEMPT_TIMES = 5;
	
	public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
		super(0);
		this.pc = pc;
		this.logger = logger;
		this.tm = tm;
		pageIndex = new PageIndex();
	}
	
	@Override
	public DataItem read(long uid) throws Exception {
		DataItemImpl di = (DataItemImpl) get(uid);
		if (!di.isValid()) {
			di.release();
			return null;
		}
		return di;
	}
	
	@Override
	public long insert(long xid, byte[] data) throws Exception {
		byte[] raw = DataItem.wrapDataItemRaw(data);
		if (raw.length > PageX.MAX_FREE_SPACE) {
			throw Error.DataTooLargeException;
		}
		
		PageInfo pageInfo = null;
		for (int i = 0; i < ATTEMPT_TIMES; i++) {
			pageInfo = pageIndex.select(data.length);
			if (pageInfo != null) {
				break;
			} else {
				// 添加新的一页，并在下次尝试获取
				int newPageNo = pc.newPage(PageX.initRaw());
				pageIndex.add(newPageNo, PageX.MAX_FREE_SPACE);
			}
		}
		if (pageInfo == null) {
			throw Error.DatabaseBusyException;
		}
		
		Page page = null;
		try {
			page = pc.getPage(pageInfo.pageNo);
			byte[] log = Recover.insertLog(xid, page, raw);
			logger.log(log);
			
			short offset = PageX.insert(page, raw);
			page.release();
			return Types.addressToUid(pageInfo.pageNo, offset);
		} finally {
			// 将page重新插入pageIndex
			if (page != null) {
				pageIndex.add(pageInfo.pageNo, PageX.getFreeSpace(page));
			} else {
				pageIndex.add(pageInfo.pageNo, 0);
			}
		}
	}
	
	@Override
	protected DataItem getForCache(long uid) throws Exception {
		short offset = (short) (uid & ((1 << 16) - 1));
		int pageNo = (int) ((uid >>> 32) & ((1L << 32) - 1));
		
		Page page = pc.getPage(pageNo);
		return DataItem.parseDataItem(page, offset, this);
	}
	
	@Override
	protected void releaseForCache(DataItem obj) {
		obj.page().release();
	}
	
	@Override
	public void close() {
		super.close();
		logger.close();
		
		PageOne.setVcClose(pageOne);
		pageOne.release();
		pc.close();
	}
	
	public void logDataItem(long xid, DataItem di) {
		byte[] log = Recover.updateLog(xid, di);
		logger.log(log);
	}
	
	public void releaseDataItem(DataItem di) {
		super.release(di.getUid());
	}
	
	void fillPageIndex() {
		int pageNum = pc.getPageNumber();
		for (int i = 2; i <= pageNum; i++) {
			Page p = null;
			try {
				p = pc.getPage(i);
			} catch (Exception e) {
				Panic.panic(e);
			}
			
			pageIndex.add(i, PageX.getFreeSpace(p));
			p.release();
		}
	}
	
	void initPageOne() {
		int pageNo = pc.newPage(PageOne.initRaw());
		assert pageNo == 1;
		
		try {
			pageOne = pc.getPage(pageNo);
		} catch (Exception e) {
			Panic.panic(e);
		}
		pc.flushPage(pageOne);
	}
	
	boolean loadCheckPageOne() {
		try {
			pageOne = pc.getPage(1);
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		return PageOne.checkVc(pageOne);
	}
}
