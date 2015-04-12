package poke.resources.data.DAO;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import poke.resources.ImageResource;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

public class ImageDAO {
	private static String bucketName = "cmpe275";
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

			File downloadFile = new File(downloadFileName);
			OutputStream outputStream = new BufferedOutputStream(
					new FileOutputStream(downloadFile));
			boolean success = ftpClient
					.retrieveFile(remoteFileName, outputStream);
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

/*
 * try { //System.out.println("Uploading a new object to S3 from a file\n");
 * //System.out.println(uploadFileName); File file = new File(uploadFileName);
 * //System.out.println(file.exists()); s3client.putObject(new
 * PutObjectRequest(bucketName, keyName, file)); file.delete();
 * 
 * } catch (AmazonServiceException ase) {
 * System.out.println("Caught an AmazonServiceException, which " +
 * "means your request made it " +
 * "to Amazon S3, but was rejected with an error response" +
 * " for some reason."); System.out.println("Error Message:    " +
 * ase.getMessage()); System.out.println("HTTP Status Code: " +
 * ase.getStatusCode()); System.out.println("AWS Error Code:   " +
 * ase.getErrorCode()); System.out.println("Error Type:       " +
 * ase.getErrorType()); System.out.println("Request ID:       " +
 * ase.getRequestId()); result = false; } catch (AmazonClientException ace) {
 * System.out.println("Caught an AmazonClientException, which " +
 * "means the client encountered " + "an internal error while trying to " +
 * "communicate with S3, " + "such as not being able to access the network.");
 * System.out.println("Error Message: " + ace.getMessage()); result = false; }
 * finally { return result; }
 */

/*
 * try { System.out.println("Downloading an object");
 * 
 * GetObjectRequest request = new GetObjectRequest(bucketName, keyName);
 * S3Object object = s3Client.getObject(request); S3ObjectInputStream
 * objectContent = object.getObjectContent(); IOUtils.copy(objectContent, new
 * FileOutputStream(downloadFileName));
 * 
 * S3Object s3object = s3Client.getObject(new GetObjectRequest( bucketName,
 * keyName));
 * 
 * System.out.println("Printing bytes retrieved.");
 * 
 * } catch (AmazonServiceException ase) {
 * System.out.println("Caught an AmazonServiceException, which" +
 * " means your request made it " +
 * "to Amazon S3, but was rejected with an error response" +
 * " for some reason."); System.out.println("Error Message:    " +
 * ase.getMessage()); System.out.println("HTTP Status Code: " +
 * ase.getStatusCode()); System.out.println("AWS Error Code:   " +
 * ase.getErrorCode()); System.out.println("Error Type:       " +
 * ase.getErrorType()); System.out.println("Request ID:       " +
 * ase.getRequestId()); result = false; } catch (AmazonClientException ace) {
 * System.out.println("Caught an AmazonClientException, which means" +
 * " the client encountered " + "an internal error while trying to " +
 * "communicate with S3, " + "such as not being able to access the network.");
 * System.out.println("Error Message: " + ace.getMessage()); result = false; }
 * catch (Exception e) {
 * 
 * System.out.println(e); result = false; } finally { return result; }
 */

