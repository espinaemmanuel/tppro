package ar.uba.fi.tppro.core.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Properties;

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
import ar.uba.fi.tppro.core.broker.VersionGenerator;
import ar.uba.fi.tppro.core.index.IndexCoreHandler;
import ar.uba.fi.tppro.core.index.LocalNodeDescriptor;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.ClusterManager.ClusterManager;
import ar.uba.fi.tppro.core.index.ClusterManager.ZkClusterManager;
import ar.uba.fi.tppro.core.index.ClusterManager.ClusterManager.NodeType;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.lock.NullLockManager;
import ar.uba.fi.tppro.core.index.versionTracker.LocalShardVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.ZkShardVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.partition.LocalPartitionResolver;
import ar.uba.fi.tppro.partition.PartitionResolver;
import ar.uba.fi.tppro.partition.ZookeeperPartitionResolver;
import ar.uba.fi.tppro.util.NetworkUtils;

public class BrokerServer implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexBroker.class);
	
	protected int port;
	protected File dataDir;

	protected IndexBrokerHandler handler;
	protected IndexBroker.Processor<IndexBroker.Iface> processor;
	private boolean localMode;
	
	protected CuratorFramework curator;
	private IndexCoreHandler localIndexServer;
	private ClusterManager clusterManager;
	private String zookeeperHost;

	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		Properties props  = new Properties(System.getProperties());
		
		File propertiesFile = new File("cluster.properties");
		if(propertiesFile.exists()){
			System.out.println("Loading properties from " + propertiesFile);
			props.load(new FileReader("cluster.properties"));
		} else {
			System.out.println("Using system properties");
		}
		
		boolean localMode = Boolean.parseBoolean(props.getProperty("brokerLocalMode", "false"));
		String port = props.getProperty("brokerPort", "9091");
		String dataDir = props.getProperty("brokerDataDir", "data");
		String zookeeper = System.getProperty("zookeeper", "localhost:2181");

		new Thread(new BrokerServer(Integer.parseInt(port), new File(dataDir), zookeeper, localMode)).start();
	}
	
	public BrokerServer(int port, File dataDir, String zookeeper, boolean localMode){
		this.port = port;
		this.dataDir = dataDir;
		this.localMode = localMode;
		zookeeperHost = zookeeper;
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
				GroupVersionTracker versionTracker = new LocalShardVersionTracker();
				
				LocalNodeDescriptor descriptor = new LocalNodeDescriptor();
				
				this.localIndexServer = new IndexCoreHandler(descriptor, partitionResolver, versionTracker, lockManager);
				descriptor.setlocalIndex(this.localIndexServer);
				
				this.localIndexServer.open(this.dataDir, true);
				
				if(!this.localIndexServer.containsPartition(1, 1)){
					this.localIndexServer.createPartition(1, 1);
				}
						
				handler = new IndexBrokerHandler(partitionResolver, lockManager, versionTracker, new VersionGenerator());

			} else {
				//Distributed mode				
				if(this.curator == null){
					if(zookeeperHost == null){
						System.out.println("Zookeeper server not specified");
						return;
					}
					
					curator = createZookeeperClient(zookeeperHost);
					curator.start();
				}
				
				PartitionResolver partitionResolver = new ZookeeperPartitionResolver(curator);
				GroupVersionTracker versionTracker = new ZkShardVersionTracker(curator);
				LockManager lockManager = new NullLockManager();
				
				handler = new IndexBrokerHandler(partitionResolver, lockManager, versionTracker, new VersionGenerator());
				
				logger.info("Registering node in zookeeper");
				Collection<InetAddress> ips = NetworkUtils.getAllLocalIPs();
				if (ips.size() == 0) {
					throw new Exception("Could not retrieve the local ip");
				}
				String address = ips.iterator().next().getHostAddress();
				
				if(this.clusterManager == null){
					this.clusterManager = new ZkClusterManager(curator);
				}
				
				RemoteIndexNodeDescriptor nodeDescriptor = new RemoteIndexNodeDescriptor(address, port);
				clusterManager.registerNode(nodeDescriptor, NodeType.BROKER);
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
	
	public CuratorFramework getCurator() {
		return curator;
	}

	public void setCurator(CuratorFramework curator) {
		this.curator = curator;
	}

	public ClusterManager getClusterManager() {
		return clusterManager;
	}

	public void setClusterManager(ClusterManager clusterManager) {
		this.clusterManager = clusterManager;
	}
}
