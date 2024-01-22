package backend.dm.pageIndex;

/**
 * @date 2023/12/5
 * @package backend.dm.pageIndex
 */
public class PageInfo {
	
	public int pageNo;
	public int freeSpace;
	
	public PageInfo(int pageNo, int freeSpace) {
		this.pageNo = pageNo;
		this.freeSpace = freeSpace;
	}
}
