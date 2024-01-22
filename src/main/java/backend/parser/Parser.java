package backend.parser;

import backend.parser.statement.*;
import common.Error;

import java.util.*;

/**
 * @date 2024/1/17
 * @package backend.parser.statement
 */
public class Parser {
	public static Object parse(byte[] statement) throws Exception {
		Tokenizer tokenizer = new Tokenizer(statement);
		String token = tokenizer.peek();
		tokenizer.pop();
		
		Object stat = null;
		Exception statErr = null;
		
		try {
			stat = switch (token) {
				case "begin" -> parseBegin(tokenizer);
				case "commit" -> parseCommit(tokenizer);
				case "abort" -> parseAbort(tokenizer);
				case "create" -> parseCreate(tokenizer);
				case "drop" -> parseDrop(tokenizer);
				case "select" -> parseSelect(tokenizer);
				case "insert" -> parseInsert(tokenizer);
				case "delete" -> parseDelete(tokenizer);
				case "update" -> parseUpdate(tokenizer);
				case "show" -> parseShow(tokenizer);
				default -> throw Error.InvalidCommandException;
			};
		} catch (Exception e) {
			statErr = e;
		}
		
		try {
			String next = tokenizer.peek();
			if (!"".equals(next)) {
				byte[] errStat = tokenizer.errStat();
				statErr = new RuntimeException("Invalid statement: " + Arrays.toString(errStat));
			}
		} catch (Exception e) {
			e.printStackTrace();
			byte[] errStat = tokenizer.errStat();
			statErr = new RuntimeException("Invalid statement: " + Arrays.toString(errStat));
		}
		
		if (statErr != null) {
			throw statErr;
		}
		return stat;
	}
	
	private static Show parseShow(Tokenizer tokenizer) throws Exception {
		String tmp = tokenizer.peek();
		if ("".equals(tmp)) {
			return new Show();
		}
		throw Error.InvalidCommandException;
	}
	
	private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
		Update update = new Update();
		update.tableName = tokenizer.peek();
		tokenizer.pop();
		
		if (!"set".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		update.fieldName = tokenizer.peek();
		tokenizer.pop();
		
		update.value = tokenizer.peek();
		tokenizer.pop();
		
		if ("".equals(tokenizer.peek())) {
			update.where = null;
		} else {
			update.where = parseWhere(tokenizer);
		}
		
		return update;
	}
	
	private static Where parseWhere(Tokenizer tokenizer) throws Exception {
		Where where = new Where();
		
		if (!"where".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		where.singleExp1 = parseSingleExp(tokenizer);
		String logicOp = tokenizer.peek();
		if("".equals(logicOp)) {
			where.logicOp = logicOp;
			return where;
		}
		if(!isLogicOp(logicOp)) {
			throw Error.InvalidCommandException;
		}
		where.logicOp = logicOp;
		tokenizer.pop();
		
		where.singleExp2 = parseSingleExp(tokenizer);
		
		if(!"".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		return where;
	}
	
	private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
		SingleExpression exp = new SingleExpression();
		
		String field = tokenizer.peek();
		if (!isName(field)) {
			throw Error.InvalidCommandException;
		}
		exp.field = field;
		tokenizer.pop();
		
		String op = tokenizer.peek();
		if(!isCmpOp(op)) {
			throw Error.InvalidCommandException;
		}
		exp.compareOp = op;
		tokenizer.pop();
		
		exp.value = tokenizer.peek();
		tokenizer.pop();
		return exp;
	}
	
	private static boolean isCmpOp(String op) {
		return "=".equals(op) || ">".equals(op) || "<".equals(op);
	}
	
	private static boolean isLogicOp(String op) {
		return "and".equals(op) || "or".equals(op);
	}
	
	private static boolean isName(String name) {
		return name.length() > 0 && Tokenizer.isAlphaBeta(name.getBytes()[0]);
	}
	
	private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
		Delete delete = new Delete();
		
		if(!"from".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		String tableName = tokenizer.peek();
		if(!isName(tableName)) {
			throw Error.InvalidCommandException;
		}
		delete.tableName = tableName;
		tokenizer.pop();
		
		delete.where = parseWhere(tokenizer);
		return delete;
	}
	
	private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
		Insert insert = new Insert();
		
		if(!"into".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		String tableName = tokenizer.peek();
		if(!isName(tableName)) {
			throw Error.InvalidCommandException;
		}
		insert.tableName = tableName;
		tokenizer.pop();
		
		if(!"values".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		
		List<String> values = new ArrayList<>();
		while(true) {
			tokenizer.pop();
			String value = tokenizer.peek();
			if("".equals(value)) {
				break;
			} else {
				values.add(value);
			}
		}
		insert.values = values.toArray(new String[0]);
		
		return insert;
	}
	
	private static Select parseSelect(Tokenizer tokenizer) throws Exception {
		Select read = new Select();
		
		List<String> fields = new ArrayList<>();
		String asterisk = tokenizer.peek();
		if("*".equals(asterisk)) {
			fields.add(asterisk);
			tokenizer.pop();
		} else {
			while(true) {
				String field = tokenizer.peek();
				if(!isName(field)) {
					throw Error.InvalidCommandException;
				}
				fields.add(field);
				tokenizer.pop();
				if(",".equals(tokenizer.peek())) {
					tokenizer.pop();
				} else {
					break;
				}
			}
		}
		read.fields = fields.toArray(new String[0]);
		
		if(!"from".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		String tableName = tokenizer.peek();
		if(!isName(tableName)) {
			throw Error.InvalidCommandException;
		}
		read.tableName = tableName;
		tokenizer.pop();
		
		String tmp = tokenizer.peek();
		if("".equals(tmp)) {
			read.where = null;
			return read;
		}
		
		read.where = parseWhere(tokenizer);
		return read;
	}
	
	
	private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
		if(!"table".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		String tableName = tokenizer.peek();
		if(!isName(tableName)) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		if(!"".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		
		Drop drop = new Drop();
		drop.tableName = tableName;
		return drop;
	}
	
	private static boolean isType(String tp) {
		return ("int32".equals(tp) || "int64".equals(tp) ||
						"string".equals(tp));
	}
	
	private static Create parseCreate(Tokenizer tokenizer) throws Exception {
		if(!"table".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		Create create = new Create();
		String name = tokenizer.peek();
		if(!isName(name)) {
			throw Error.InvalidCommandException;
		}
		create.tableName = name;
		
		List<String> fNames = new ArrayList<>();
		List<String> fTypes = new ArrayList<>();
		while(true) {
			tokenizer.pop();
			String field = tokenizer.peek();
			if("(".equals(field)) {
				break;
			}
			
			if(!isName(field)) {
				throw Error.InvalidCommandException;
			}
			
			tokenizer.pop();
			String fieldType = tokenizer.peek();
			if(!isType(fieldType)) {
				throw Error.InvalidCommandException;
			}
			fNames.add(field);
			fTypes.add(fieldType);
			tokenizer.pop();
			
			String next = tokenizer.peek();
			if(",".equals(next)) {
				continue;
			} else if("".equals(next)) {
				throw Error.TableNoIndexException;
			} else if("(".equals(next)) {
				break;
			} else {
				throw Error.InvalidCommandException;
			}
		}
		create.fieldName = fNames.toArray(new String[0]);
		create.fieldType = fTypes.toArray(new String[0]);
		
		tokenizer.pop();
		if(!"index".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		
		Set<String> indexes = new HashSet<>();
		while(true) {
			tokenizer.pop();
			String field = tokenizer.peek();
			if(")".equals(field)) {
				break;
			}
			if(!isName(field)) {
				throw Error.InvalidCommandException;
			} else {
				indexes.add(field);
			}
		}
		create.index = indexes;
		tokenizer.pop();
		
		if(!"".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		return create;
	}
	
	private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
		if(!"".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		return new Abort();
	}
	
	private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
		if(!"".equals(tokenizer.peek())) {
			throw Error.InvalidCommandException;
		}
		return new Commit();
	}
	
	private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
		String isolation = tokenizer.peek();
		Begin begin = new Begin();
		if("".equals(isolation)) {
			return begin;
		}
		if(!"isolation".equals(isolation)) {
			throw Error.InvalidCommandException;
		}
		
		tokenizer.pop();
		String level = tokenizer.peek();
		if(!"level".equals(level)) {
			throw Error.InvalidCommandException;
		}
		tokenizer.pop();
		
		String tmp1 = tokenizer.peek();
		if("read".equals(tmp1)) {
			tokenizer.pop();
			String tmp2 = tokenizer.peek();
			if("committed".equals(tmp2)) {
				tokenizer.pop();
				if(!"".equals(tokenizer.peek())) {
					throw Error.InvalidCommandException;
				}
				return begin;
			} else {
				throw Error.InvalidCommandException;
			}
		} else if("repeatable".equals(tmp1)) {
			tokenizer.pop();
			String tmp2 = tokenizer.peek();
			if("read".equals(tmp2)) {
				begin.isRepeatableRead = true;
				tokenizer.pop();
				if(!"".equals(tokenizer.peek())) {
					throw Error.InvalidCommandException;
				}
				return begin;
			} else {
				throw Error.InvalidCommandException;
			}
		} else {
			throw Error.InvalidCommandException;
		}
	}
}
