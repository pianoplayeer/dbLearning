package backend.dm.page;

/**
 * @date 2023/12/2
 * @package backend.dm.page
 */

import backend.dm.pageCache.PageCache;
import backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理页
 * db启动时在100-107字节处插入一个随机数，db关闭时，将其拷贝到108-115
 * 检验上次数据库是否正常关闭
 */
public class PageOne {
	private static final int OF_VC = 100;
	private static final int LEN_VC = 8;
	
	public static byte[] initRaw() {
		byte[] raw = new byte[PageCache.PAGE_SIZE];
		setVcOpen(raw);
		return raw;
	}
	
	public static void setVcOpen(Page page) {
		page.setDirty(true);
		setVcOpen(page.getData());
	}
	
	private static void setVcOpen(byte[] raw) {
		System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
	}
	
	public static void setVcClose(Page page) {
		page.setDirty(true);
		setVcClose(page.getData());
	}
	
	private static void setVcClose(byte[] raw) {
		System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
	}
	
	private static boolean checkVc(byte[] raw) {
		return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
	}
	
	public static boolean checkVc(Page page) {
		return checkVc(page.getData());
	}
}