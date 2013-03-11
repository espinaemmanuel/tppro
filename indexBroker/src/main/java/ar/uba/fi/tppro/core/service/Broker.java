package ar.uba.fi.tppro.core.service;

import java.io.File;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;

import ar.uba.fi.tppro.core.broker.IndexBrokerHandler;
import ar.uba.fi.tppro.core.index.IndexPartitionsGroup;
import ar.uba.fi.tppro.core.index.LocalNodeDescriptor;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.lock.NullLockManager;
import ar.uba.fi.tppro.core.index.versionTracker.LocalShardVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.ShardVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.ZkShardVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.partition.LocalPartitionResolver;
import ar.uba.fi.tppro.partition.PartitionResolver;
import ar.uba.fi.tppro.partition.ZookeeperPartitionResolver;

public class Broker implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexBroker.class);
	
	protected int port;
	protected File dataDir;

	protected IndexBrokerHandler handler;
	protected IndexBroker.Processor<IndexBroker.Iface> processor;
	private boolean localMode;
	
	private IndexPartitionsGroup localIndexServer;

	public static void main(String[] args) {
		
		boolean localMode = System.getProperties().containsKey("localMode");
		String port = System.getProperty("port", "9090");
		String dataDir = System.getProperty("dataDir", "./data");

		new Thread(new Broker(Integer.parseInt(port), new File(dataDir), localMode)).start();
	}
	
	public Broker(int port, File dataDir, boolean localMode){
		this.port = port;
		this.dataDir = dataDir;
		this.localMode = localMode;
	}
	
	private static CuratorFramework createZookeeperClient(String zookeeperHost) {
		return CuratorFrameworkFactory.newClient(zookeeperHost, new RetryOneTime(1));
	}

	@Override
	public void run() {
		try {
			if(this.localMode){
				PartitionResolver partitionResolver = new LocalPartitionResolver();
				NullLockManager lockManager = new NullLockManager();
				ShardVersionTracker versionTracker = new LocalShardVersionTracker();
				
				LocalNodeDescriptor descriptor = new LocalNodeDescriptor();
				
				this.localIndexServer = new IndexPartitionsGroup(descriptor, partitionResolver, versionTracker, lockManager);
				descriptor.setlocalIndex(this.localIndexServer);
				
				this.localIndexServer.open(this.dataDir, false);
				
				if(!this.localIndexServer.containsPartition(1, 1)){
					this.localIndexServer.createPartition(1, 1);
				}
						
				handler = new IndexBrokerHandler(partitionResolver, lockManager, versionTracker);

			} else {
				//Distributed mode
				String zookeeperHost = System.getProperty("zookeeper");
				if(zookeeperHost == null){
					System.out.println("Zookeeper server not specified");
					return;
				}
				
				CuratorFramework curatorClient = createZookeeperClient(zookeeperHost);
				curatorClient.start();
				
				PartitionResolver partitionResolver = new ZookeeperPartitionResolver(curatorClient);
				ShardVersionTracker versionTracker = new ZkShardVersionTracker(curatorClient);
				LockManager lockManager = null;
				
				handler = new IndexBrokerHandler(partitionResolver, lockManager, versionTracker);
			}
			
			
			processor = new IndexBroker.Processor<IndexBroker.Iface>(handler);
			
			TServerTransport serverTransport = new TServerSocket(this.port);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(
					serverTransport).processor(processor));
			
			logger.info("Starting the Index broker on port " + this.port + "...");
			server.serve();

		} catch (Exception e) {
			logger.error("Fatal error", e);
		}
	}
}
