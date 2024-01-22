package backend.tbm;

/**
 * @date 2024/1/19
 * @package backend.tbm
 */

import backend.parser.statement.*;
import backend.tm.TransactionManagerImpl;
import backend.utils.Panic;
import backend.utils.ParseStringRes;
import backend.utils.Parser;
import com.google.common.primitives.Bytes;
import common.Error;

import java.util.*;

/**
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */

public class Table {
	TableManager tbm;
	long uid;
	String name;
	byte status;
	long nextUid;
	List<Field> fields = new ArrayList<>();
	
	public static Table loadTable(TableManager tbm, long uid) {
		byte[] raw = null;
		try {
			raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		assert raw != null;
		Table tb = new Table(tbm, uid);
		return tb.parseSelf(raw);
	}
	
	public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
		Table tb = new Table(tbm, create.tableName, nextUid);
		for (int i = 0; i < create.fieldName.length; i++) {
			String fieldName = create.fieldName[i];
			String fieldType = create.fieldType[i];
			boolean indexed = create.index.contains(fieldName);
			
			tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
		}
		
		return tb.persistSelf(xid);
	}
	
	public Table (TableManager tbm, long uid) {
		this.tbm = tbm;
		this.uid = uid;
	}
	
	public Table(TableManager tbm, String tableName, long nextUid) {
		this.tbm = tbm;
		this.name = tableName;
		this.nextUid = nextUid;
	}
	
	private Table parseSelf(byte[] raw) {
		int pos = 0;
		ParseStringRes res = Parser.parseString(raw);
		name = res.str;
		pos += res.next;
		
		nextUid = Parser.parseLong(Arrays.copyOfRange(raw, pos, pos + 8));
		pos += 8;
		
		while (pos < raw.length) {
			long uid = Parser.parseLong(Arrays.copyOfRange(raw, pos, pos + 8));
			pos += 8;
			fields.add(Field.loadField(this, uid));
		}
		return this;
	}
	
	private Table persistSelf(long xid) throws Exception {
		byte[] nameRaw = Parser.string2Byte(name);
		byte[] nextRaw = Parser.long2Byte(nextUid);
		byte[] fieldRaw = new byte[0];
		for (Field field : fields) {
			fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
		}
		
		uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
		return this;
	}
	
	private List<Long> parseWhere(Where where) throws Exception {
		long l1 = 0, r1 = 0, l2 = 0, r2 = 0;
		boolean single = false;
		Field field = null;
		
		if (where == null) {
			single = true;
			l1 = 0;
			r1 = Long.MAX_VALUE;
			
			for (Field f : fields) {
				if (f.isIndexed()) {
					field = f;
					break;
				}
			}
			assert field != null : Error.FieldNotFoundException;
		} else {
			for (Field f : fields) {
				if (f.fieldName.equals(where.singleExp1.field)) {
					assert f.isIndexed() : Error.FieldNotIndexedException;
					field = f;
					break;
				}
			}
			
			assert field != null : Error.FieldNotFoundException;
			CalWhereRes res = calWhere(field, where);
			l1 = res.l1;
			l2 = res.l2;
			r1 = res.r1;
			r2 = res.r2;
			single = res.single;
		}
		
		List<Long> uids = field.search(l1, r1);
		if (!single) {
			uids.addAll(field.search(l2, r2));
		}
		
		return uids;
	}
	
	public int delete(long xid, Delete delete) throws Exception {
		List<Long> uids = parseWhere(delete.where);
		int count = 0;
		
		for (long uid : uids) {
			if (((TableManagerImpl) tbm).vm.delete(xid, uid)) {
				count++;
			}
		}
		
		return count;
	}
	
	public int update(long xid, Update update) throws Exception {
		List<Long> uids = parseWhere(update.where);
		Field field = null;
		
		for (Field f : fields) {
			if (f.fieldName.equals(update.fieldName)) {
				field = f;
				break;
			}
		}
		
		assert field != null : Error.FieldNotFoundException;
		Object value = field.string2Value(update.value);
		int count = 0;
		
		for (Long uid : uids) {
			byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
			if (raw == null) {
				continue;
			}
			
			((TableManagerImpl)tbm).vm.delete(xid, uid);
			Map<String, Object> entry = parseEntry(raw);
			entry.put(field.fieldName, value);
			raw = entry2Raw(entry);
			long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
			count++;
			
			for (Field f : fields) {
				if (f.isIndexed()) {
					field.insert(entry.get(field.fieldName), uuid);
				}
			}
		}
		
		return count;
	}
	
	public String read(long xid, Select read) throws Exception {
		List<Long> uids = parseWhere(read.where);
		StringBuilder sb = new StringBuilder();
		for (Long uid : uids) {
			byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
			if(raw == null) {
				continue;
			}
			
			Map<String, Object> tmp = parseEntry(raw);
			Map<String, Object> entry = new HashMap<>();
			
			for (String field : read.fields) {
				entry.put(field, tmp.get(field));
			}
			sb.append(printEntry(entry)).append("\n");
		}
		return sb.toString();
	}
	
	public void insert(long xid, Insert insert) throws Exception {
		Map<String, Object> entry = string2Entry(insert.values);
		byte[] raw = entry2Raw(entry);
		long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
		for (Field field : fields) {
			if(field.isIndexed()) {
				field.insert(entry.get(field.fieldName), uid);
			}
		}
	}
	
	private Map<String, Object> string2Entry(String[] values) throws Exception {
		assert values.length != fields.size() : Error.InvalidValuesException;
		Map<String, Object> entry = new HashMap<>();
		
		for (int i = 0; i < fields.size(); i++) {
			Field f = fields.get(i);
			entry.put(f.fieldName, f.string2Value(values[i]));
		}
		
		return entry;
	}
	
	class CalWhereRes {
		long l1, r1;
		long l2, r2;
		boolean single;
	}
	
	private CalWhereRes calWhere(Field field, Where where) throws Exception {
		CalWhereRes res = new CalWhereRes();
		switch (where.logicOp) {
			case "" -> {
				res.single = true;
				FieldCalRes r = field.calExp(where.singleExp1);
				res.l1 = r.left;
				res.r1 = r.right;
			}
			
			case "or" -> {
				res.single = false;
				FieldCalRes r = field.calExp(where.singleExp1);
				res.l1 = r.left;
				res.r1 = r.right;
				
				r = field.calExp(where.singleExp2);
				res.l2 = r.left;
				res.r2 = r.right;
			}
			
			case "and" -> {
				res.single = true;
				FieldCalRes r = field.calExp(where.singleExp1);
				res.l1 = r.left;
				res.r1 = r.right;
				
				r = field.calExp(where.singleExp2);
				res.l2 = r.left;
				res.r2 = r.right;
				
				res.l1 = Math.max(res.l1, res.l2);
				res.r1 = Math.min(res.r1, res.r2);
			}
			
			default -> {
				throw Error.InvalidLogicOpException;
			}
		}
		
		return res;
	}
	
	private String printEntry(Map<String, Object> entry) {
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < fields.size(); i++) {
			Field field = fields.get(i);
			sb.append(field.printValue(entry.get(field.fieldName)));
			if(i == fields.size()-1) {
				sb.append("]");
			} else {
				sb.append(", ");
			}
		}
		return sb.toString();
	}
	
	private Map<String, Object> parseEntry(byte[] raw) {
		int pos = 0;
		Map<String, Object> entry = new HashMap<>();
		
		for (Field f : fields) {
			Field.ParseValueRes res = f.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
			entry.put(f.fieldName, res.v);
			pos += res.shift;
		}
		
		return entry;
	}
	
	private byte[] entry2Raw(Map<String, Object> entry) {
		byte[] raw = new byte[0];
		for (Field f : fields) {
			raw = Bytes.concat(raw, f.value2Raw(entry.get(f.fieldName)));
		}
		
		return raw;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("{");
		sb.append(name).append(": ");
		for(Field field : fields) {
			sb.append(field.toString());
			if(field == fields.get(fields.size()-1)) {
				sb.append("}");
			} else {
				sb.append(", ");
			}
		}
		return sb.toString();
	}
}
