package backend.dm.page;

/**
 * @date 2023/12/2
 * @package backend.dm.page
 */
public interface Page {
	void lock();
	void unlock();
	void release();
	void setDirty(boolean dirty);
	boolean isDirty();
	int getPageNumber();
	byte[] getData();
}
