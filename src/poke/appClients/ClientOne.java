package poke.appClients;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import com.google.protobuf.ByteString;

import poke.client.ClientCommand;
import poke.client.ClientPrintListener;
import poke.client.comm.CommListener;

public class ClientOne {
	private String tag;

	public ClientOne(String tag) {
		this.tag = tag;
	}

	public void run(ClientCommand cc) {

		try {

			byte[] myByeImage;
			BufferedImage originalImage = ImageIO.read(new File(
					"/home/ampatel/Downloads/netty.jpg"));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(originalImage, "jpg", baos);
			baos.flush();
			myByeImage = baos.toByteArray();
			baos.close();

			ByteString bs = ByteString.copyFrom(myByeImage);

			cc.sendImage("1", "My First Image", bs);
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		try {
			ClientCommand cc = new ClientCommand("localhost", 5570);
			CommListener listener = new ClientPrintListener("First Client");
			cc.addListener(listener);

			ClientOne cone = new ClientOne("FirstClient");
			cone.run(cc);
			// we are running asynchronously
			System.out.println("\nExiting in 150 seconds");
			Thread.sleep(150000);
			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
