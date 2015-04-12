package poke.resources.data.DAO;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import poke.resources.ImageResource;

public class ImageStoreFTP implements ImageStore {
	FTPClient ftpClient = new FTPClient();
	String server = "10.0.1.2";
	int port = 21;
	String user = "public";
	String pass = "";

	public boolean uploadImage(String keyName) {
		String uploadFileName = ImageResource.imagePath + keyName + ".png";

		boolean result = false;

		try {

			ftpClient.connect(server, port);
			ftpClient.login(user, pass);
			ftpClient.enterLocalPassiveMode();

			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			File firstLocalFile = new File(uploadFileName);

			String firstRemoteFile = keyName + ".png";
			InputStream inputStream = new FileInputStream(firstLocalFile);

			System.out.println("Uploading file...");
			boolean done = ftpClient.storeFile(firstRemoteFile, inputStream);
			inputStream.close();

			if (done) {
				System.out.println("File uploaded successfully!!");
				result = true;
			}

		} catch (IOException ex) {
			System.out.println("Error: " + ex.getMessage());
			ex.printStackTrace();
			return false;
		} finally {
			try {
				if (ftpClient.isConnected()) {
					ftpClient.logout();
					ftpClient.disconnect();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				return false;
			}

		}
		return result;
	}

	public boolean getImage(String keyName) {
		String downloadFileName = ImageResource.imagePath + keyName + ".png";

		boolean result = false;

		try {
			ftpClient.connect(server, port);
			ftpClient.login(user, pass);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			String remoteFileName = keyName + ".png";
			
			System.out.println("Downloading file...");
			File downloadFile = new File(downloadFileName);
			OutputStream outputStream = new BufferedOutputStream(
					new FileOutputStream(downloadFile));
			boolean success = ftpClient.retrieveFile(remoteFileName,
					outputStream);
			outputStream.close();

			if (success) {
				System.out.println("File downloaded successfully!!");
				result = true;
			}

		} catch (IOException ex) {
			System.out.println("Error: " + ex.getMessage());
			ex.printStackTrace();
			return false;
		} finally {
			try {
				if (ftpClient.isConnected()) {
					ftpClient.logout();
					ftpClient.disconnect();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				return false;
			}
		}
		return result;
	}
}
