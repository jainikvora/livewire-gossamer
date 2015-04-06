package poke.resources;

import poke.comm.Image.Request;
import poke.server.queue.PerChannelQueue;

public interface ClientResource {

	public void  process(Request request,PerChannelQueue channel);
}
