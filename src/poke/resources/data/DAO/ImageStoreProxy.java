package poke.resources.data.DAO;

public class ImageStoreProxy implements ImageStore{
	public enum ImageStoreMethod {
		S3, FTP;
	}
	private String methodUsed;
	private ImageStoreFTP imageStoreFTP;
	private ImageStoreS3 imageStoreS3;
	
	public ImageStoreProxy(String method) {
		this.methodUsed = method;
		imageStoreFTP = new ImageStoreFTP();
		imageStoreS3 = new ImageStoreS3();
	}

	@Override
	public boolean uploadImage(String keyName) {
		boolean success = false;
		switch(methodUsed) {
			case "S3":
				success = imageStoreS3.uploadImage(keyName);
				break;
			case "FTP":
				success = imageStoreFTP.uploadImage(keyName);
				break;
			default:
				break;
		}
		return success;
	}

	@Override
	public boolean getImage(String keyName) {
		boolean success = false;
		switch(methodUsed) {
			case "S3":
				success = imageStoreS3.getImage(keyName);
				break;
			case "FTP":
				success = imageStoreFTP.getImage(keyName);
				break;
			default:
				break;
		}
		return success;
	}

}
