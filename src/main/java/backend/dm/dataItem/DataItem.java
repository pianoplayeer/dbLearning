package backend.dm.dataItem;

import backend.common.SubArray;
import backend.dm.DataManagerImpl;
import backend.dm.page.Page;
import backend.utils.Parser;
import backend.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * @date 2023/12/4
 * @package backend.dm.dataItem
 */
public interface DataItem {
	SubArray data();
	
	void before();
	void unBefore();
	void after(long xid);
	void release();
	
	void lock();
	void unlock();
	void rLock();
	void rUnlock();
	
	Page page();
	long getUid();
	byte[] getOldRaw();
	SubArray getRaw();
	
	static void setDataItemRawInvalid(byte[] raw) {
		raw[DataItemImpl.OF_VALID] = 1;
	}
	
	/**
	 * 结构：[valid: 1][dataSize: 2][data]
	 * valid 0为合法，1为非法
	 */
	static byte[] wrapDataItemRaw(byte[] raw) {
		byte[] valid = new byte[1];
		byte[] size = Parser.short2Byte((short) raw.length);
		
		return Bytes.concat(valid, size, raw);
	}
	
	/**
	 *
	 * 解析出页面page在offset处的DataItem
	 */
	static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
		byte[] raw = page.getData();
		short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
		int len = size + DataItemImpl.OF_DATA;
		long uid = Types.addressToUid(page.getPageNumber(), offset);
		
		return new DataItemImpl(new SubArray(raw, offset, offset + len), new byte[len], page, uid, dm);
	}
	
}
