package poke.server.conf;

import static org.junit.Assert.*;

import org.junit.Test;

import poke.server.conf.ServerList.TCPAddress;

public class ServerListTest {

	@Test
	public void test() {
		for(TCPAddress addr : ServerList.getInstance().addresses){
			System.out.println(addr.host + ", " + addr.port);
		}
		
	}

}
