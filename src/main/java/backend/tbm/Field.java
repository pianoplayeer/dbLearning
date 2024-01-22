package backend.tbm;

/**
 * @date 2024/1/19
 * @package backend.tbm
 */

import backend.dm.DataManager;
import backend.im.BPlusTree;
import backend.parser.statement.SingleExpression;
import backend.tm.TransactionManagerImpl;
import backend.utils.Panic;
import backend.utils.ParseStringRes;
import backend.utils.Parser;
import com.google.common.primitives.Bytes;
import common.Error;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

/**
 * [FieldName][TypeName][IndexUid]
 * 若field无索引，IndexUid为0
 *
 * 字符串存储方式: [StringLength: 4][StringData]
 */
public class Field {
	long uid;
	private Table tb;
	String fieldName;
	String fieldType;
	private long index; // 对应的索引B+树的bootUid
	private BPlusTree tree;
	
	public static Field loadField(Table tb, long uid) {
		byte[] raw = null;
		try {
			raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
		} catch (Exception e) {
			Panic.panic(e);
		}
		
		assert raw != null;
		return new Field(uid, tb).parseSelf(raw);
	}
	
	public Field(long uid, Table tb) {
		this.uid = uid;
		this.tb = tb;
	}
	
	public Field(Table tb, String fieldName, String fieldType, long index) {
		this.tb = tb;
		this.fieldName = fieldName;
		this.fieldType = fieldType;
		this.index = index;
	}
	
	private Field parseSelf(byte[] raw) {
		int pos = 0;
		ParseStringRes res = Parser.parseString(raw);
		fieldName = res.str;
		pos += res.next;
		
		res = Parser.parseString(Arrays.copyOfRange(raw, pos, raw.length));
		fieldType = res.str;
		pos += res.next;
		
		this.index = Parser.parseLong(Arrays.copyOfRange(raw, pos, pos + 8));
		if (index != 0) {
			try {
				tree = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
			} catch (Exception e) {
				Panic.panic(e);
			}
		}
		
		return this;
	}
	
	private static void typeCheck(String fieldType) throws Exception {
		if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
			throw Error.InvalidFieldException;
		}
	}
	
	public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
		typeCheck(fieldType);
		Field f = new Field(tb, fieldName, fieldType, 0);
		if (indexed) {
			DataManager dm = ((TableManagerImpl)tb.tbm).dm;
			long index = BPlusTree.create(dm);
			
			f.tree = BPlusTree.load(index, dm);
			f.index = index;
		}
		
		f.persistSelf(xid);
		return f;
	}
	
	private void persistSelf(long xid) throws Exception {
		byte[] nameRaw = Parser.string2Byte(fieldName);
		byte[] typeRaw = Parser.string2Byte(fieldType);
		byte[] indexRaw = Parser.long2Byte(index);
		
		this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
	}
	
	public boolean isIndexed() {
		return index != 0;
	}
	
	public void insert(Object key, long uid) throws Exception {
		long uKey = value2Uid(key);
		tree.insert(uKey, uid);
	}
	
	public List<Long> search(long left, long right) throws Exception {
		return tree.searchRange(left, right);
	}
	
	public Object string2Value(String str) {
		return switch (fieldType) {
			case "int32" -> Integer.parseInt(str);
			case "int64" -> Long.parseLong(str);
			case "string" -> str;
			default -> null;
		};
	}
	
	public long value2Uid(Object key) {
		long uid = 0;
		switch (fieldType) {
			case "string" -> uid = Parser.str2Uid((String) key);
			case "int32" -> uid = (int) key;
			case "int64" -> uid = (long) key;
		}
		return uid;
	}
	
	public byte[] value2Raw(Object v) {
		return switch (fieldType) {
			case "int32" -> Parser.int2Byte((int) v);
			case "int64" -> Parser.long2Byte((long) v);
			case "string" -> Parser.string2Byte((String) v);
			default -> null;
		};
	}
	
	class ParseValueRes {
		Object v;
		int shift;
	}
	
	public ParseValueRes parseValue(byte[] raw) {
		ParseValueRes res = new ParseValueRes();
		switch (fieldType) {
			case "int32" -> {
				res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
				res.shift = 4;
			}
			case "int64" -> {
				res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
				res.shift = 8;
			}
			case "string" -> {
				ParseStringRes r = Parser.parseString(raw);
				res.v = r.str;
				res.shift = r.next;
			}
		}
		return res;
	}
	
	public String printValue(Object v) {
		return switch (fieldType) {
			case "int32" -> String.valueOf((int) v);
			case "int64" -> String.valueOf((long) v);
			case "string" -> (String) v;
			default -> null;
		};
	}
	
	@Override
	public String toString() {
		return "(" +
					   fieldName +
					   ", " +
					   fieldType +
					   (index != 0 ? ", Index" : ", NoIndex") +
					   ")";
	}
	
	public FieldCalRes calExp(SingleExpression exp) throws Exception {
		Object v = null;
		FieldCalRes res = new FieldCalRes();
		switch (exp.compareOp) {
			case "<" -> {
				res.left = 0;
				v = string2Value(exp.value);
				res.right = value2Uid(v);
				if (res.right > 0) {
					res.right--;
				}
			}
			case "=" -> {
				v = string2Value(exp.value);
				res.left = value2Uid(v);
				res.right = res.left;
			}
			case ">" -> {
				res.right = Long.MAX_VALUE;
				v = string2Value(exp.value);
				res.left = value2Uid(v) + 1;
			}
		}
		return res;
	}
}
