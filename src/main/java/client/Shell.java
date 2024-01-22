package client;

import lombok.AllArgsConstructor;
import org.checkerframework.checker.units.qual.A;

import java.util.Scanner;

/**
 * @date 2024/1/21
 * @package client
 */
@AllArgsConstructor
public class Shell {
	private Client client;
	
	public void run() {
		
		try (Scanner sc = new Scanner(System.in)) {
			while (true) {
				System.out.print(":> ");
				String stat = sc.nextLine();
				
				if ("exit".equals(stat) || "quit".equals(stat)) {
					break;
				}
				
				try {
					byte[] res = client.execute(stat.getBytes());
					System.out.println(new String(res));
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
		} finally {
			client.close();
		}
	}
}
