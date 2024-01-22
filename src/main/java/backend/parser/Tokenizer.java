package backend.parser;

import common.Error;

/**
 * @date 2024/1/17
 * @package backend.parser
 */
public class Tokenizer {
	private byte[] stat;
	private int pos;
	private String currentToken;
	private boolean flushToken;
	private Exception err;
	
	public Tokenizer(byte[] stat) {
		this.stat = stat;
		pos = 0;
		currentToken = "";
		this.flushToken = true;
	}
	
	public void pop() {
		flushToken = true;
	}
	
	public byte[] errStat() {
		byte[] res = new byte[stat.length + 3];
		System.arraycopy(stat, 0, res, 0, pos);
		System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
		System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
		
		return res;
	}
	
	private void popByte() {
		pos++;
		if (pos > stat.length) {
			pos = stat.length;
		}
	}
	
	private Byte peekByte() {
		if (pos == stat.length) {
			return null;
		}
		return stat[pos];
	}
	
	public String peek() throws Exception {
		if (err != null) {
			throw err;
		}
		
		if (flushToken) {
			String token = null;
			try {
				token = next();
			} catch (Exception e) {
				err = e;
				throw e;
			}
			
			currentToken = token;
			flushToken = false;
		}
		return currentToken;
	}
	
	private String next() throws Exception {
		if (err != null) {
			throw err;
		}
		return nextMetaState();
	}
	
	private String nextMetaState() throws Exception {
		while (true) {
			Byte b = peekByte();
			if (b == null) {
				return "";
			}
			
			if (!isBlank(b)) {
				break;
			}
			popByte();
		}
		
		Byte b = peekByte();
		assert b != null;
		
		if (isSymbol(b)) {
			popByte();
			return new String(new byte[]{b});
		} else if (b == '"' || b == '\'') {
			return nextQuoteState();
		} else if (isAlphaBeta(b) || isDigit(b)) {
			return nextTokenState();
		} else {
			err = Error.InvalidCommandException;
			throw err;
		}
	}
	
	private String nextTokenState() throws Exception {
		StringBuilder builder = new StringBuilder();
		
		while (true) {
			Byte b = peekByte();
			if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
				if (b != null && isBlank(b)) {
					popByte();
				}
				return builder.toString();
			}
			
			builder.append(new String(new byte[]{b}));
			popByte();
		}
	}
	
	private String nextQuoteState() throws Exception {
		Byte quote = peekByte();
		assert quote != null;
		popByte();
		StringBuilder builder = new StringBuilder();
		
		while (true) {
			Byte b = peekByte();
			if (b == null) {
				err = Error.InvalidCommandException;
				throw err;
			}
			
			if (b.equals(quote)) {
				popByte();
				break;
			}
			
			builder.append(b);
			popByte();
		}
		return builder.toString();
	}
	
	static boolean isDigit(byte b) {
		return (b >= '0' && b <= '9');
	}
	
	static boolean isAlphaBeta(byte b) {
		return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
	}
	
	static boolean isSymbol(byte b) {
		return (b == '>' || b == '<' || b == '=' || b == '*' ||
						b == ',' || b == '(' || b == ')');
	}
	
	static boolean isBlank(byte b) {
		return (b == '\n' || b == ' ' || b == '\t');
	}
}
