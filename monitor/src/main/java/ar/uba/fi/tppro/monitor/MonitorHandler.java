package ar.uba.fi.tppro.monitor;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.curator.framework.CuratorFramework;

import ar.uba.fi.tppro.core.index.versionTracker.VersionTrackerServerException;
import ar.uba.fi.tppro.core.index.versionTracker.ZkShardVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.Node;
import ar.uba.fi.tppro.core.service.thrift.NodePartition;
import ar.uba.fi.tppro.core.service.thrift.Monitor;
import ar.uba.fi.tppro.core.service.thrift.NodeType;
import ar.uba.fi.tppro.partition.PartitionDescriptor;
import ar.uba.fi.tppro.partition.PartitionResolverException;
import ar.uba.fi.tppro.partition.ZookeeperPartitionResolver;

public class MonitorHandler implements Monitor.Iface {

	final Logger logger = LoggerFactory.getLogger(MonitorHandler.class);

	private CuratorFramework curator;
	private ZookeeperPartitionResolver partitionResolver;
	private ZkShardVersionTracker versionTracker;

	public MonitorHandler(CuratorFramework curator) {
		this.curator = curator;

		this.partitionResolver = new ZookeeperPartitionResolver(this.curator);
		this.versionTracker = new ZkShardVersionTracker(this.curator);
	}

	public List<NodePartition> getReplicas() throws TException {

		List<NodePartition> replicas = Lists.newArrayList();

		try {
			for (PartitionDescriptor replica : this.partitionResolver.getAll()) {
				NodePartition nodePartition = new NodePartition(replica.group,
						replica.partition, replica.url,
						replica.status.toString());
				replicas.add(nodePartition);
			}
		} catch (PartitionResolverException e) {
			throw new TException("error getting the partition status", e);
		}

		return replicas;
	}

	public Map<Integer, String> getGroupVersion() throws TException {

		try {
			Map<Integer, Long> versionMap = this.versionTracker
					.getAllVersions();
			Map<Integer, String> returnMap = Maps.newHashMap();

			for (Integer group : versionMap.keySet()) {
				returnMap.put(group, versionMap.get(group).toString());
			}

			return returnMap;
		} catch (VersionTrackerServerException e) {
			throw new TException("error accessing version map", e);
		}
	}

	public List<Node> getNodes() throws TException {
		
		List<Node> nodes = Lists.newArrayList();
		
		try {
			List<String> brokers = this.curator.getChildren().forPath("/brokers");
			
			if(brokers != null){
				for(String url : brokers){
					nodes.add(new Node(url, NodeType.BROKER));
				}
			}
		} catch (Exception e) {
			logger.info("no broker registered");
		}
		
		try {
			List<String> clusters = this.curator.getChildren().forPath("/cluster");
			
			if(clusters != null){
				for(String url : clusters){
					nodes.add(new Node(url, NodeType.INDEX));
				}
			}
		} catch (Exception e) {
			logger.info("no cores registered");
		}
		
		return nodes;
	}

}
