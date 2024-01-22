package backend.im;

import backend.common.SubArray;
import backend.dm.DataManager;
import backend.dm.dataItem.DataItem;
import backend.tm.TransactionManagerImpl;
import backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date 2024/1/16
 * @package backend.im
 */
public class BPlusTree {
	DataManager dm;
	long bootUid;
	DataItem bootDataItem;
	Lock bootLock;
	
	public static long create(DataManager dm) throws Exception {
		byte[] rawRoot = Node.newNilRootRaw();
		long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
		return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
	}
	
	public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
		DataItem bootDataItem = dm.read(bootUid);
		assert bootDataItem != null;
		
		BPlusTree t = new BPlusTree();
		t.bootDataItem = bootDataItem;
		t.dm = dm;
		t.bootUid = bootUid;
		t.bootLock = new ReentrantLock();
		
		return t;
	}
	
	private long rootUid() {
		bootLock.lock();
		try {
			SubArray data = bootDataItem.data();
			return Parser.parseLong(Arrays.copyOfRange(data.raw, data.start, data.end));
		} finally {
			bootLock.unlock();
		}
	}
	
	private void updateRootUid(long left, long right, long rightKey) throws Exception {
		bootLock.lock();
		try {
			byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
			long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
			bootDataItem.before();
			SubArray data = bootDataItem.data();
			System.arraycopy(Parser.long2Byte(newRootUid), 0, data.raw, data.start, 8);
			bootDataItem.after(TransactionManagerImpl.SUPER_XID);
		} finally {
			bootLock.unlock();
		}
	}
	
	private long searchLeaf(long nodeUid, long key) throws Exception {
		Node node = Node.loadNode(this, nodeUid);
		node.release();
		
		if (node.isLeaf()) {
			return nodeUid;
		} else {
			long next = searchNext(nodeUid, key);
			return searchLeaf(next, key);
		}
	}
	
	private long searchNext(long nodeUid, long key) throws Exception {
		while (true) {
			Node node = Node.loadNode(this, nodeUid);
			Node.SearchNextRes res = node.searchNext(key);
			node.release();
			
			if (res.uid != 0) {
				return res.uid;
			}
			nodeUid = res.siblingUid;
		}
	}
	
	public List<Long> search(long key) throws Exception {
		return searchRange(key, key);
	}
	
	public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
		long rootUid = rootUid();
		long leafUid = searchLeaf(rootUid, leftKey);
		List<Long> uids = new ArrayList<>();
		
		while (true) {
			Node leaf = Node.loadNode(this, leafUid);
			Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
			leaf.release();
			uids.addAll(res.uidList);
			
			if (res.siblingUid == 0) {
				break;
			} else {
				leafUid = res.siblingUid;
			}
		}
		return uids;
	}
	
	public void insert(long key, long uid) throws Exception {
		long rootUid = rootUid();
		InsertRes res = insert(rootUid, uid, key);
		assert res != null;
		
		if (res.newNode != 0) {
			updateRootUid(rootUid, res.newNode, res.newKey);
		}
	}
	
	class InsertRes {
		long newNode;
		long newKey;
	}
	
	private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
		Node node = Node.loadNode(this, nodeUid);
		node.release();
		
		InsertRes res;
		if (node.isLeaf()) {
			res = insertAndSplit(nodeUid, uid, key);
		} else {
			long next = searchNext(nodeUid, key);
			InsertRes nextRes = insert(next, uid, key);
			
			if (nextRes.newNode != 0) {
				res = insertAndSplit(nodeUid, nextRes.newNode, nextRes.newKey);
			} else {
				res = new InsertRes();
			}
		}
		
		return res;
	}
	
	private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
		while (true) {
			Node node = Node.loadNode(this, nodeUid);
			Node.InsertAndSplitRes r = node.insertAndSplit(uid, key);
			node.release();
			
			if (r.siblingUid != 0) {
				nodeUid = r.siblingUid;
			} else {
				InsertRes res = new InsertRes();
				res.newNode = r.newSon;
				res.newKey = r.newKey;
				return res;
			}
		}
	}
	
	public void close() {
		bootDataItem.release();
	}
}
