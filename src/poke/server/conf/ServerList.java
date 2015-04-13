package poke.server.conf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class ServerList {
	public List<TCPAddress> addresses;
	private static ServerList instance;
	private String filePath;
	
	private ServerList() {
		filePath = "../../resources/server_list.conf";
		addresses = new ArrayList<TCPAddress>();
		populateAddresses();
	}
	
	private void populateAddresses() {
		File file = new File(filePath);
		if(file.exists()) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(file));
				String line = br.readLine();
				while(line != null) {
					String host = line.split(",")[0];
					String port = line.split(",")[1];
					addresses.add(new TCPAddress(host,Integer.parseInt(port)));
					line = br.readLine();
				}
				br.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else {
			System.out.println("No such file exists");
		}
		
	}
	public static ServerList getInstance() {
		if(instance == null)
			instance = new ServerList();
		return instance;
	}
	
	public class TCPAddress{
		public String host;
		public int port;
		public TCPAddress(String host, int port) {
			this.host = host;
			this.port = port;
		}
	}
}
