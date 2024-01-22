package backend.tbm;

import backend.dm.DataManager;
import backend.parser.statement.*;
import backend.utils.Parser;
import backend.vm.VersionManager;
import common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2024/1/19
 * @package backend.tbm
 */
public class TableManagerImpl implements TableManager {
	VersionManager vm;
	DataManager dm;
	private Booter booter;
	private Map<String, Table> tableCache;
	private Map<Long, List<Table>> xidTableCache;
	private Lock lock;
	
	public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
		this.vm = vm;
		this.dm = dm;
		this.booter = booter;
		this.tableCache = new HashMap<>();
		this.xidTableCache = new HashMap<>();
		lock = new ReentrantLock();
		loadTable();
	}
	
	private void loadTable() {
		long uid = firstTableUid();
		while (uid != 0) {
			Table tb = Table.loadTable(this, uid);
			tableCache.put(tb.name, tb);
			uid = tb.nextUid;
		}
	}
	
	private long firstTableUid() {
		byte[] raw = booter.load();
		return Parser.parseLong(raw);
	}
	
	private void updateFirstTableUid(long uid) {
		byte[] raw = Parser.long2Byte(uid);
		booter.update(raw);
	}
	
	@Override
	public BeginRes begin(Begin begin) {
		BeginRes res = new BeginRes();
		int level = begin.isRepeatableRead ? 1 : 0;
		res.xid = vm.begin(level);
		res.result = "begin".getBytes();
		return res;
	}
	
	@Override
	public byte[] commit(long xid) throws Exception {
		vm.commit(xid);
		return "commit".getBytes();
	}
	
	@Override
	public byte[] abort(long xid) {
		vm.abort(xid);
		return "abort".getBytes();
	}
	
	@Override
	public byte[] show(long xid) {
		lock.lock();
		try {
			StringBuilder builder = new StringBuilder();
			
			for (Table tb : tableCache.values()) {
				builder.append(tb.toString()).append("\n");
			}
			
			List<Table> list = xidTableCache.get(xid);
			if (list == null) {
				return "\n".getBytes();
			}
			
			for (Table tb : list) {
				builder.append(tb.toString()).append("\n");
			}
			
			return builder.toString().getBytes();
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public byte[] create(long xid, Create create) throws Exception {
		lock.lock();
		try {
			assert !tableCache.containsKey(create.tableName) : Error.DuplicatedTableException;
			Table table = Table.createTable(this, firstTableUid(), xid, create);
			updateFirstTableUid(table.uid);
			tableCache.put(table.name, table);
			
			if (!xidTableCache.containsKey(xid)) {
				xidTableCache.put(xid, new ArrayList<>());
			}
			xidTableCache.get(xid).add(table);
			
			return ("create " + table.name).getBytes();
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public byte[] insert(long xid, Insert insert) throws Exception {
		lock.lock();
		Table table = tableCache.get(insert.tableName);
		lock.unlock();
		
		assert table != null : Error.TableNotFoundException;
		table.insert(xid, insert);
		
		return "insert".getBytes();
	}
	
	@Override
	public byte[] read(long xid, Select select) throws Exception {
		lock.lock();
		Table table = tableCache.get(select.tableName);
		lock.unlock();
		if(table == null) {
			throw Error.TableNotFoundException;
		}
		return table.read(xid, select).getBytes();
	}
	
	@Override
	public byte[] update(long xid, Update update) throws Exception {
		lock.lock();
		Table table = tableCache.get(update.tableName);
		lock.unlock();

		assert table != null : Error.TableNotFoundException;
		int count = table.update(xid, update);
		return ("update " + count).getBytes();
	}
	
	@Override
	public byte[] delete(long xid, Delete delete) throws Exception {
		lock.lock();
		Table table = tableCache.get(delete.tableName);
		lock.unlock();
		
		assert table != null : Error.TableNotFoundException;
		int count = table.delete(xid, delete);
		return ("delete " + count).getBytes();
	}
	
	// TODO: 还未实现表的删除
	@Override
	public byte[] drop(long xid, Drop drop) throws Exception {
		Delete delete = new Delete();
		delete.tableName = drop.tableName;
		delete(xid, delete);
		
		return ("drop " + drop.tableName).getBytes();
	}
}
