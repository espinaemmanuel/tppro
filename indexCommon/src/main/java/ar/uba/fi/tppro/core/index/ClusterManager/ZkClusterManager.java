package ar.uba.fi.tppro.core.index.ClusterManager;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

public class ZkClusterManager implements ClusterManager {
	
	private CuratorFramework curator;
	
	final static Logger logger = LoggerFactory
			.getLogger(ZkClusterManager.class);
	
	public ZkClusterManager(CuratorFramework curator){
		this.curator = curator;
	}

	@Override
	public void registerNode(IndexNodeDescriptor nodeDescriptor,
			NodeType index) throws Exception {
		curator.create().creatingParentsIfNeeded()
		.withMode(CreateMode.EPHEMERAL)
		.forPath("/cluster/" + nodeDescriptor.getHost() + "_" + nodeDescriptor.getPort(), null);
logger.debug("Node registered in zookeeper " + nodeDescriptor.getHost() + ":"
		+ nodeDescriptor.getPort());

	}

}
