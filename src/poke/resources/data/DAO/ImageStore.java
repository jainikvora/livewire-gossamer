package poke.resources.data.DAO;

public interface ImageStore {
	public boolean uploadImage(String keyName);
	public boolean getImage(String keyName);
}
