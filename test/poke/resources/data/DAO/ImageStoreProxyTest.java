package poke.resources.data.DAO;

import static org.junit.Assert.*;

import org.junit.Test;

import poke.resources.data.DAO.ImageStoreProxy.ImageStoreMethod;

public class ImageStoreProxyTest {

	@Test
	public void test() {
		ImageStoreProxy imageStore = new ImageStoreProxy(ImageStoreMethod.S3);
		imageStore.getImage("test");
	}

}
