package backend;

import backend.dm.DataManager;
import backend.server.Server;
import backend.tbm.TableManager;
import backend.tm.TransactionManager;
import backend.utils.Panic;
import backend.vm.VersionManager;
import common.Error;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * @date 2024/1/20
 * @package backend
 */

@CommandLine.Command(name = "launcher", mixinStandardHelpOptions = true, version = "launcher 1.0",
					description = "Open or create a DB Path.")
public class Launcher implements Callable<Integer> {
	
	private static final int port = 9999;
	private static final long DEFAULT_MEM = (1 << 20) * 64;
	
	private static final long KB = 1 << 10;
	private static final long MB = 1 << 20;
	private static final long GB = 1 << 20;
	
	
	@CommandLine.Option(names = {"-c", "--create"}, description = "Create the DB Path")
	boolean create;
	
	@CommandLine.Option(names = {"-o", "--open"}, description = "Open the DB Path")
	boolean open;
	
	@CommandLine.Parameters(index = "0", description = "The path where the DB files will be created or exists.")
	private String path;
	
	@CommandLine.Option(names = "-mem", description = "Assign the memory size. E.g. -mem NKB/MB/GB.")
	private String mem;
	
	@Override
	public Integer call() {
		if (create) {
			createDB(path, parseMem(mem));
		}
		
		if (open) {
			openDB(path, parseMem(mem));
		}
		
		return 0;
	}
	
	private static void createDB(String path, long mem) {
		TransactionManager tm = TransactionManager.create(path);
		DataManager dm = DataManager.create(path, mem, tm);
		VersionManager vm = VersionManager.newVersionManager(tm, dm);
		TableManager.create(path, vm, dm);
		
		tm.close();
		dm.close();
	}
	
	private static void openDB(String path, long mem) {
		TransactionManager tm = TransactionManager.open(path);
		DataManager dm = DataManager.open(path, mem, tm);
		VersionManager vm = VersionManager.newVersionManager(tm, dm);
		TableManager tbm = TableManager.open(path, vm, dm);
		
		new Server(port, tbm).start();
	}
	
	private static long parseMem(String mem) {
		if (mem == null || "".equals(mem)) {
			return DEFAULT_MEM;
		}
		
		if (mem.length() < 3) {
			Panic.panic(Error.InvalidMemException);
		}
		
		String unit = mem.substring(mem.length() - 2);
		long memNum = Long.parseLong(mem.substring(0, mem.length() - 2));
		
		switch (unit) {
			case "KB":
				return memNum * KB;
			case "MB":
				return memNum * MB;
			case "GB":
				return memNum * GB;
			default:
				Panic.panic(Error.InvalidMemException);
		}
		
		return DEFAULT_MEM;
	}
	
	public static void main(String[] args) {
		System.exit(new CommandLine(new Launcher()).execute(args));
	}
}
