package backend.dm.page;

/**
 * @date 2023/12/2
 * @package backend.dm.page
 */

import backend.dm.pageCache.PageCache;
import backend.utils.Parser;

import java.util.Arrays;

/**
 * 正常页结构
 * [FSO(Free Space Offset)][Data]
 * FSO: 2字节
 */
public class PageX {
	
	private static final short OF_FREE = 0;
	private static final short OF_DATA = 2;
	public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;
	
	public static byte[] initRaw() {
		byte[] raw = new byte[PageCache.PAGE_SIZE];
		setFSO(raw, OF_DATA);
		return raw;
	}
	
	public static void setFSO(byte[] raw, short ofData) {
		System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
	}
	
	private static short getFSO(byte[] raw) {
		return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
	}
	
	public static short getFSO(Page page) {
		return getFSO(page.getData());
	}
	
	/**
	 * 进行数据的插入
	 * @param page 待插入的页
	 * @param raw 插入的数据
	 * @return 插入位置
	 */
	public static short insert(Page page, byte[] raw) {
		page.setDirty(true);
		short offset = getFSO(page);
		System.arraycopy(raw, 0, page.getData(), offset, raw.length);
		setFSO(page.getData(), (short) (offset + raw.length));
		
		return offset;
	}
	
	public static int getFreeSpace(Page page) {
		return PageCache.PAGE_SIZE - getFSO(page);
	}
	
	public static void recoverInsert(Page page, byte[] raw, short offset) {
		page.setDirty(true);
		System.arraycopy(raw, 0, page.getData(), offset, raw.length);
		
		short rawOffset = getFSO(page);
		if (rawOffset < offset + raw.length) {
			setFSO(page.getData(), (short) (offset + rawOffset));
		}
	}
	
	public static void recoverUpdate(Page page, byte[] raw, short offset) {
		page.setDirty(true);
		System.arraycopy(raw, 0, page.getData(), offset, raw.length);
	}
}
