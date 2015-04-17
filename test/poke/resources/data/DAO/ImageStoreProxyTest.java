package poke.resources.data.DAO;

import org.junit.Test;

public class ImageStoreProxyTest {

	@Test
	public void test() {
		ImageStoreProxy imageStore = new ImageStoreProxy("S3");
		imageStore.getImage("test");
	}

}
