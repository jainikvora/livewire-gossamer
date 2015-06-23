Distributed Snapchat-ish servers using Raft consensus algorithm and Netty

	• The high-level topology of the multi-cluster system mirrors the gossamer (spider-mesh) architecture.
	• Goal of the project was to build highly available, consistent and scalable distributed system.
	• Highly Available: 
		○ Multiple servers running same application and serving the clients preventing any downtime
	• Consistent:
		○ Raft algorithm with leader election and log replication was implemented to provide required consistency, avoid single point of failure.
		○ Elected leader was responsible to replicate the log among majority of servers and send data to other clusters connected in the network
		○ Heartbeat between leader and followers was instrumental to detect leader failure and start new election
	• Scalable:
		○ Use of common standard i.e. standardized protobuf messages enabled the system to add any number of nodes or even clusters.
	• Multithreaded Java application with server implemented using Netty (Java NIO) and Protobuff for messaging.
	• Storage of image was done by Amazon S3 and/or FTP. Implemented proxy design pattern to enable switching of image storage strategies at run time.
	• Implemented client connection management  to provide Single System abstraction
