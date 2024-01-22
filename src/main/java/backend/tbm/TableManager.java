package backend.tbm;

import backend.dm.DataManager;
import backend.parser.statement.*;
import backend.utils.Parser;
import backend.vm.VersionManager;

/**
 * @date 2024/1/19
 * @package backend.tbm
 */
public interface TableManager {
	BeginRes begin(Begin begin);
	byte[] commit(long xid) throws Exception;
	byte[] abort(long xid);
	
	byte[] show(long xid);
	byte[] create(long xid, Create create) throws Exception;
	
	byte[] insert(long xid, Insert insert) throws Exception;
	byte[] read(long xid, Select select) throws Exception;
	byte[] update(long xid, Update update) throws Exception;
	byte[] delete(long xid, Delete delete) throws Exception;
	byte[] drop(long xid, Drop drop) throws Exception;
	
	static TableManager create(String path, VersionManager vm, DataManager dm) {
			Booter booter = Booter.create(path);
			booter.update(Parser.long2Byte(0));
			return new TableManagerImpl(vm, dm, booter);
	}
	
	static TableManager open(String path, VersionManager vm, DataManager dm) {
		Booter booter = Booter.open(path);
		return new TableManagerImpl(vm, dm, booter);
	}
}
