package poke.appClients;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.UUID;

import javax.imageio.ImageIO;

import com.google.protobuf.ByteString;

import poke.client.ClientCommand;
import poke.client.ClientPrintListener;
import poke.client.comm.CommListener;

public class ClientThree {

	int clientID;

	public ClientThree(int clientID) {
		this.clientID = clientID;
	}

	public void run(ClientCommand cc, String imageName) {

		try {

			byte[] myByeImage;
			BufferedImage originalImage = ImageIO.read(new File(
					"../../resources/clientSendImages/" + imageName));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(originalImage, "png", baos);
			baos.flush();
			myByeImage = baos.toByteArray();
			baos.close();

			String uniqueRequestID = UUID.randomUUID().toString()
					.replaceAll("-", "");
			ByteString bs = ByteString.copyFrom(myByeImage);
			cc.sendImage(uniqueRequestID, "My Image" + imageName, bs, clientID);

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		try {

			Scanner sc = new Scanner(System.in);

			ClientCommand cc = new ClientCommand("localhost", 5570);
			CommListener listener = new ClientPrintListener("Third Client");
			cc.addListener(listener);

			ClientThree cone = new ClientThree(3);
			String clientInput = "";

			do {

				System.out.println("Do you want to send an image (Y/N)?");
				clientInput = sc.nextLine();

				if (clientInput != null && "Y".equalsIgnoreCase(clientInput)) {

					System.out.println("Sending Image ");
					cone.run(cc, "1.png");
				}

			}

			while (!"N".equalsIgnoreCase(clientInput));

			// we are running asynchronously
			System.out.println("\nExiting in few seconds");
			Thread.sleep(150000000);
			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
