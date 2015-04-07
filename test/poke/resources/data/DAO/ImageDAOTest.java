package poke.resources.data.DAO;

import static org.junit.Assert.*;

import org.junit.Test;

public class ImageDAOTest {

	@Test
	public void test() {
		ImageDAO dao = new ImageDAO();
		if(dao.getImage("test")) {
			System.out.println("Image uploaded successfully");
		}else {
			System.out.println("Error while uploading image");
		}
		//fail("Not yet implemented");
	}

}
