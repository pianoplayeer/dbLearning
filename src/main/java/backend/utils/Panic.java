package backend.utils;

/**
 * @date 2023/11/29
 * @package PACKAGE_NAME
 */
public class Panic {
	
	public static void panic(Exception e) {
		e.printStackTrace();
		System.exit(1);
	}
}
