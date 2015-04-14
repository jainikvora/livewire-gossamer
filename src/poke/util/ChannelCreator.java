package poke.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.HashMap;
import java.util.Map;

import poke.client.comm.CommConnection.ClientClosedListener;
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
	
	public boolean createChannelToNode(TCPAddress address){
		
		/*EventLoopGroup bossGroup = new NioEventLoopGroup();
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
*/
		
		EventLoopGroup group = new NioEventLoopGroup();
		try {
		
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(new ServerInitializer(false));
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			System.out.println("Starting server " + address.host + ", listening on port = " + address.port);
			// Make the connection attempt.
			ChannelFuture channel = b.connect(address.host, address.port).syncUninterruptibly();
			if(channel.isDone() && channel.isSuccess()){
				allNodeChannels.put(address, channel.channel());
				System.out.println(channel.isSuccess());
				System.out.println("channel established");
				return true;
				}
			else{
				System.out.println("Channel false..");
				return false;
			}
			
			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			//ClientClosedListener ccl = new ClientClosedListener(this,address);
			//channel.channel().closeFuture().addListener(ccl);
			//channel.channel().closeFuture().sync();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			return false;
		}
	}

}
