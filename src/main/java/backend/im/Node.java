package backend.im;

import backend.common.SubArray;
import backend.dm.dataItem.DataItem;
import backend.tm.TransactionManagerImpl;
import backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @date 2024/1/16
 * @package backend.im
 * <p>
 * 结构：
 * [isLeaf: 1][numKeys: 2][sibling: 8]
 * [son0][key0][son1][key1]...[sonN][keyN]
 * <p>
 * keyN作为边界设为MAX_VALUE
 */
public class Node {
	static final int IS_LEAF_OFFSET = 0;
	static final int NUM_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
	static final int SIBLING_OFFSET = NUM_KEYS_OFFSET + 2;
	static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;
	
	static final int BALANCE_NUMBER = 32;
	static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * 2 * BALANCE_NUMBER;
	
	BPlusTree tree;
	DataItem dataItem;
	SubArray raw;
	long uid;
	
	static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
		int val = isLeaf ? 1 : 0;
		raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) val;
	}
	
	static boolean getRawIsLeaf(SubArray raw) {
		return raw.raw[raw.start + IS_LEAF_OFFSET] == 1;
	}
	
	static void setRawNumKeys(SubArray raw, int numKeys) {
		System.arraycopy(Parser.short2Byte((short) numKeys), 0, raw.raw, raw.start + NUM_KEYS_OFFSET, 2);
	}
	
	static int getRawNumKeys(SubArray raw) {
		return Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NUM_KEYS_OFFSET, raw.start + NUM_KEYS_OFFSET + 2));
	}
	
	static void setRawSibling(SubArray raw, long sibling) {
		System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
	}
	
	static long getRawSibling(SubArray raw) {
		return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
	}
	
	static void setRawKthSon(SubArray raw, long uid, int kth) {
		int offset = raw.start + NODE_HEADER_SIZE + kth * (2 * 8);
		System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
	}
	
	static long getRawKthSon(SubArray raw, int kth) {
		int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
		return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
	}
	
	static void setRawKthKey(SubArray raw, long key, int kth) {
		int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
		System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
	}
	
	static long getRawKthKey(SubArray raw, int kth) {
		int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
		return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
	}
	
	static void copyRawFromKth(SubArray from, SubArray to, int kth) {
		int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
		System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
	}
	
	static void shiftRawKth(SubArray raw, int kth) {
		int begin = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
		int end = raw.start + NODE_SIZE;
		
		if (end - begin > 0) {
			System.arraycopy(raw.raw, begin, raw.raw, begin + 2 * 8, end - begin - 2 * 8);
		}
	}
	
	static byte[] newRootRaw(long left, long right, long key) {
		SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
		
		setRawIsLeaf(raw, false);
		setRawNumKeys(raw, 2);
		setRawSibling(raw, 0);
		setRawKthSon(raw, left, 0);
		setRawKthKey(raw, key, 0);
		setRawKthSon(raw, right, 1);
		setRawKthKey(raw, Long.MAX_VALUE, 1);
		
		return raw.raw;
	}
	
	static byte[] newNilRootRaw() {
		SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
		
		setRawIsLeaf(raw, true);
		setRawNumKeys(raw, 0);
		setRawSibling(raw, 0);
		
		return raw.raw;
	}
	
	static Node loadNode(BPlusTree btree, long uid) throws Exception {
		DataItem di = btree.dm.read(uid);
		
		assert di != null;
		Node n = new Node();
		n.tree = btree;
		n.uid = uid;
		n.dataItem = di;
		n.raw = di.data();
		
		return n;
	}
	
	public void release() {
		dataItem.release();
	}
	
	public boolean isLeaf() {
		dataItem.rLock();
		try {
			return getRawIsLeaf(raw);
		} finally {
			dataItem.rUnlock();
		}
	}
	
	class SearchNextRes {
		long uid;
		long siblingUid;
	}
	
	public SearchNextRes searchNext(long key) {
		dataItem.rLock();
		
		try {
			SearchNextRes res = new SearchNextRes();
			int numKeys = getRawNumKeys(raw);
			
			for (int i = 0; i < numKeys; i++) {
				long k = getRawKthKey(raw, i);
				
				if (key < k) {
					res.siblingUid = 0;
					res.uid = getRawKthSon(raw, i);
					return res;
				}
			}
			
			res.siblingUid = getRawSibling(raw);
			res.uid = 0;
			return res;
		} finally {
			dataItem.rUnlock();
		}
	}
	
	class LeafSearchRangeRes {
		List<Long> uidList;
		long siblingUid;
	}
	
	public LeafSearchRangeRes leafSearchRange(long left, long right) {
		dataItem.rLock();
		try {
			int numKeys = getRawNumKeys(raw);
			int k = 0;
			
			while (k < numKeys) {
				if (getRawKthKey(raw, k) >= left) {
					break;
				}
				k++;
			}
			
			List<Long> uids = new ArrayList<>();
			while (k < numKeys) {
				long son = getRawKthSon(raw, k);
				if (getRawKthKey(raw, k) <= right) {
					uids.add(son);
					k++;
				} else {
					break;
				}
			}
			
			long siblingUid = 0;
			if (k == numKeys) {
				siblingUid = getRawSibling(raw);
			}
			LeafSearchRangeRes res = new LeafSearchRangeRes();
			res.siblingUid = siblingUid;
			res.uidList = uids;
			return res;
		} finally {
			dataItem.rUnlock();
		}
	}
	
	class InsertAndSplitRes {
		long siblingUid;
		long newSon;
		long newKey;
	}
	
	private boolean needSplit() {
		return 2 * BALANCE_NUMBER == getRawNumKeys(raw);
	}
	
	public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
		boolean success = false;
		Exception err = null;
		InsertAndSplitRes res = new InsertAndSplitRes();
		
		dataItem.before();
		try {
			success = insert(uid, key);
			if (!success) {
				res.siblingUid = getRawSibling(raw);
				return res;
			}
			
			if (needSplit()) {
				try {
					SplitRes r = split();
					res.newSon = r.newSon;
					res.newKey = r.newKey;
					return res;
				} catch (Exception e) {
					err = e;
					throw e;
				}
			} else {
				return res;
			}
		} finally {
			if (err == null && success) {
				dataItem.after(TransactionManagerImpl.SUPER_XID);
			} else {
				dataItem.unBefore();
			}
		}
	}
	
	private boolean insert(long uid, long key) {
		int numKeys = getRawNumKeys(raw);
		int kth = 0;
		
		while (kth < numKeys) {
			long k = getRawKthKey(raw, kth);
			if (k < key) {
				kth++;
			} else {
				break;
			}
		}
		
		if (kth == numKeys && getRawSibling(raw) != 0) {
			return false;
		}
		
		if (getRawIsLeaf(raw)) {
			shiftRawKth(raw, kth);
			setRawKthKey(raw, key, kth);
			setRawKthSon(raw, uid, kth);
			setRawNumKeys(raw, numKeys + 1);
		} else {
			long kk = getRawKthKey(raw, kth);
			setRawKthKey(raw, key, kth);
			shiftRawKth(raw, kth + 1);
			setRawKthKey(raw, kk, kth + 1);
			setRawKthSon(raw, uid, kth + 1);
			setRawNumKeys(raw, numKeys + 1);
		}
		
		return true;
	}
	
	class SplitRes {
		long newSon;
		long newKey;
	}
	
	private SplitRes split() throws Exception {
		SubArray node = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
		setRawIsLeaf(node, getRawIsLeaf(raw));
		setRawNumKeys(node, BALANCE_NUMBER);
		setRawSibling(node, getRawSibling(raw));
		copyRawFromKth(raw, node, BALANCE_NUMBER);
		
		long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, node.raw);
		setRawNumKeys(raw, BALANCE_NUMBER);
		setRawSibling(raw, son);
		
		SplitRes res = new SplitRes();
		res.newSon = son;
		res.newKey = getRawKthKey(node, 0);
		return res;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Is leaf: ").append(getRawIsLeaf(raw)).append("\n");
		int KeyNumber = getRawNumKeys(raw);
		sb.append("KeyNumber: ").append(KeyNumber).append("\n");
		sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
		for(int i = 0; i < KeyNumber; i ++) {
			sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
		}
		return sb.toString();
	}
}
