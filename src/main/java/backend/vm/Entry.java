package backend.vm;

import backend.common.SubArray;
import backend.dm.dataItem.DataItem;
import backend.utils.Parser;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * VM向上层提供的结构
 * [XMIN][XMAX][data]
 */
public class Entry {
	
	private static final int OF_XMIN = 0;
	private static final int OF_XMAX = OF_XMIN + 8;
	private static final int OF_DATA = OF_XMAX + 8;
	
	private long uid;
	private DataItem dataItem;
	private VersionManager vm;
	
	public static Entry newEntry(VersionManager vm, DataItem di, long uid) {
		if (di == null) {
			return null;
		}
		
		Entry entry = new Entry();
		entry.uid = uid;
		entry.dataItem = di;
		entry.vm = vm;
		
		return entry;
	}
	
	public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
		DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
		return newEntry(vm, di, uid);
	}
	
	public static byte[] wrapEntry(byte[] data, long xid) {
		byte[] xmin = Parser.long2Byte(xid);
		byte[] xmax = new byte[8];
		
		return Bytes.concat(xmin, xmax, data);
	}
	
	public byte[] data() {
		dataItem.rLock();
		try {
			SubArray arr = dataItem.data();
			byte[] data = new byte[arr.end - arr.start - OF_DATA];
			System.arraycopy(arr.raw, arr.start + OF_DATA, data, 0, data.length);
			
			return data;
		} finally {
			dataItem.rUnlock();
		}
	}
	
	public void setXmax(long xid) {
		dataItem.before();
		try {
			SubArray arr = dataItem.data();
			System.arraycopy(Parser.long2Byte(xid), 0, arr.raw, arr.start + OF_XMAX, 8);
		} finally {
			dataItem.after(xid);
		}
	}
	
	public void remove() {
		dataItem.release();
	}
	
	public void release() {
		((VersionManagerImpl) vm).releaseEntry(this);
	}
	
	public long getXmin() {
		dataItem.rLock();
		try {
			SubArray arr = dataItem.data();
			return Parser.parseLong(Arrays.copyOfRange(arr.raw, arr.start + OF_XMIN, arr.end));
		} finally {
			dataItem.rUnlock();
		}
	}
	
	public long getXmax() {
		dataItem.rLock();
		try {
			SubArray arr = dataItem.data();
			return Parser.parseLong(Arrays.copyOfRange(arr.raw, arr.start + OF_XMAX, arr.end));
		} finally {
			dataItem.rUnlock();
		}
	}
	
	public long getUid() {
		return uid;
	}
}
