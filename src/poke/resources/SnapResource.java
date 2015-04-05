package poke.resources;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.Payload;
import poke.comm.App.Ping;
import poke.comm.App.PokeStatus;
import poke.comm.App.Request;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;

public class SnapResource implements Resource {

	protected static Logger logger = LoggerFactory.getLogger("server");

	public SnapResource() {
	}

	
	public Request process(Request request) {
				
		logger.info("poke: " + request.getBody().getPing().getTag());
		
		logger.info("From Snap resource handler!!");
		
		
		//AP : Processing Request
		//AP : As of now just storing it to some other place!!
		
		// TODO: Re-write properly with error handling
		try{
			
			byte[] byteImage = request.getBody().getSnapMsg().getImage().toByteArray();
			
			
			 InputStream in = new ByteArrayInputStream(byteImage);
		        BufferedImage bImageFromConvert = ImageIO.read(in);

		        ImageIO.write(bImageFromConvert, "jpg", new File(
		                "/home/ampatel/Downloads/server/new-darksouls.jpg"));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	       

		Request.Builder rb = Request.newBuilder();

		// metadata
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));

		// payload
		Payload.Builder pb = Payload.newBuilder();
		Ping.Builder fb = Ping.newBuilder();
		fb.setTag(request.getBody().getPing().getTag());
		fb.setNumber(request.getBody().getPing().getNumber());
		pb.setPing(fb.build());
		rb.setBody(pb.build());

		Request reply = rb.build();

		return reply;
	}
}
