package poke.util;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.HashMap;
import java.util.Map;

import poke.server.ServerInitializer;
import poke.server.conf.ServerList.TCPAddress;

public class ChannelCreator {
	
	public Map<TCPAddress,Channel> allNodeChannels;
	
	private static ChannelCreator channelCreator;
	
	private ChannelCreator(){
		allNodeChannels = new HashMap<TCPAddress,Channel>();
	}
	
	public static ChannelCreator getInstance(){
		if(channelCreator==null){
			 channelCreator = new ChannelCreator();
		}
		return channelCreator;
	}
	
	public void createChannelToNode(TCPAddress address){
		
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			ServerBootstrap b = new ServerBootstrap();

			b.group(bossGroup, workerGroup);
			b.channel(NioServerSocketChannel.class);
			b.option(ChannelOption.SO_BACKLOG, 100);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

			boolean compressComm = false;
			b.childHandler(new ServerInitializer(compressComm));

			// Start the server.
			System.out.println("Connecting server " + address.host + ", listening on port = " + address.port);
			ChannelFuture f = b.bind(address.port).syncUninterruptibly();
			if(f.isDone() && f.isSuccess())
				allNodeChannels.put(address, f.channel());
			// should use a future channel listener to do this step
			// allChannels.add(f.channel());

			// block until the server socket is closed.
			f.channel().closeFuture().sync();
		} catch (Exception ex) {
			// on bind().sync()
			System.out.println("Failed to setup public handler."+ex);
		} finally {
			// Shut down all event loops to terminate all threads.
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}

	}

}
