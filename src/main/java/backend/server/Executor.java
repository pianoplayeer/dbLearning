package backend.server;

import backend.parser.Parser;
import backend.parser.statement.*;
import backend.tbm.BeginRes;
import backend.tbm.TableManager;
import common.Error;

/**
 * @date 2024/1/21
 * @package server
 */
public class Executor {
	private long xid;
	private TableManager tbm;
	
	public Executor(TableManager tbm) {
		this.tbm = tbm;
		xid = 0;
	}
	
	public void close() {
		if (xid != 0) {
			System.out.println("Abnormal abort: " + xid);
			tbm.abort(xid);
		}
	}
	
	public byte[] execute(byte[] sql) throws Exception {
		System.out.println("Execute: " + new String(sql));
		Object stat = Parser.parse(sql);
		
		if (stat instanceof Begin) {
			assert xid == 0 : Error.NestedTransactionException;
			BeginRes r = tbm.begin((Begin) stat);
			xid = r.xid;
			return r.result;
		} else if (stat instanceof Commit) {
			assert xid != 0 : Error.NoTransactionException;
			byte[] res = tbm.commit(xid);
			xid = 0;
			return res;
		} else if (stat instanceof Abort) {
			assert xid != 0 : Error.NoTransactionException;
			byte[] res = tbm.abort(xid);
			xid = 0;
			return res;
		} else {
			return execute0(stat);
		}
	}
	
	private byte[] execute0(Object stat) throws Exception {
		boolean tmpTransaction = false;
		Exception e = null;
		
		if (xid == 0) {
			tmpTransaction = true;
			BeginRes r = tbm.begin(new Begin());
			xid = r.xid;
		}
		
		try {
			byte[] res = null;
			
			if (stat instanceof Show) {
				res = tbm.show(xid);
			} else if (stat instanceof Create) {
				res = tbm.create(xid, (Create) stat);
			} else if (stat instanceof Select) {
				res = tbm.read(xid, (Select) stat);
			} else if (stat instanceof Insert) {
				res = tbm.insert(xid, (Insert) stat);
			} else if (stat instanceof Delete) {
				res = tbm.delete(xid, (Delete) stat);
			} else if (stat instanceof Update) {
				res = tbm.update(xid, (Update) stat);
			} else if (stat instanceof Drop) {
				res = tbm.drop(xid, (Drop) stat);
			}
			return res;
		} catch (Exception e1) {
			e = e1;
			throw e;
		} finally {
			if (tmpTransaction) {
				if (e != null) {
					tbm.abort(xid);
				} else {
					tbm.commit(xid);
				}
				xid = 0;
			}
		}
	}
}
