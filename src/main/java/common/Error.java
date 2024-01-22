package common;

import java.lang.reflect.Executable;

/**
 * @date 2023/11/30
 * @package common
 */
public class Error {
	// tm
	public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");
	
	public static final Exception FileExistsException = new RuntimeException("File already exists!");
	public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
	public static final Exception FileCannotRWException = new RuntimeException("File cannot be read or written!");
	
	public static final Exception CacheFullException = new RuntimeException("Cache is full!");
	public static final Exception MemTooSmallException = new RuntimeException("Memory is too small!");
	public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
	
	public static final Exception DataTooLargeException = new RuntimeException("Data is too large!");
	public static final Exception DatabaseBusyException = new RuntimeException("Database is busy!");
	
	public static final Exception DeadlockException = new RuntimeException("Deadlock occurs!");
	public static final Exception NoEntryException = new RuntimeException("No such entry!");
	public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update error!");
	
	public static final Exception InvalidCommandException = new RuntimeException("Command is invalid!");
	public static final Exception TableNoIndexException = new RuntimeException("Table has no index!");
	public static final Exception InvalidFieldException = new RuntimeException("Field is invalid!");
	public static final Exception InvalidLogicOpException = new RuntimeException("Logic Operator is invalid!");
	public static final Exception FieldNotIndexedException = new RuntimeException("Field is not indexed!");
	public static final Exception FieldNotFoundException = new RuntimeException("Field is not found!");
	public static final Exception InvalidValuesException = new RuntimeException("Values are invalid!");
	public static final Exception DuplicatedTableException = new RuntimeException("Table duplicated!");
	public static final Exception TableNotFoundException = new RuntimeException("Table is not found!");
	
	public static final Exception InvalidPkgDataException = new RuntimeException("Package is invalid!");
	
	public static final Exception NestedTransactionException = new RuntimeException("Transaction cannot be nested!");
	public static final Exception NoTransactionException = new RuntimeException("No such transaction!");
	public static final Exception InvalidMemException = new RuntimeException("Memory format is invalid!");
}
