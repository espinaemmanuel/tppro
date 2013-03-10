package ar.uba.fi.tppro.partition;

import java.util.Collection;
import java.util.List;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceInstanceBuilder;

public class ZookeeperPartitionResolver implements PartitionResolver {

	private ServiceDiscovery<IndexPartitionStatus> serviceDiscovery;

	public ZookeeperPartitionResolver(CuratorFramework curatorClient) {
		serviceDiscovery = ServiceDiscoveryBuilder
				.builder(IndexPartitionStatus.class)
				.client(curatorClient)
				.basePath("partitions")
				.build();
	}

	@Override
	public void registerPartition(int partitionId,
			IndexNodeDescriptor descriptor, IndexPartitionStatus status) throws PartitionResolverException {
		
		ServiceInstanceBuilder<IndexPartitionStatus> builder;
		try {
			builder = ServiceInstance.builder();
		} catch (Exception e) {
			throw new PartitionResolverException("Could not get local ip", e);
		}
		ServiceInstance<IndexPartitionStatus> si = builder
		.address(descriptor.getHost())
		.port(descriptor.getPort())
		.name("partition_" + partitionId)
		.id("replica_" + Integer.toString(partitionId) + "_" + descriptor.toString())
		.payload(status)
		.build();
		
		try {
			serviceDiscovery.registerService(si);
		} catch (Exception e) {
			throw new PartitionResolverException("Could not register the partition", e);
		}

	}

	@Override
	public Multimap<Integer, IndexNodeDescriptor> resolve(
			List<Integer> partitionIds) throws PartitionResolverException {
		
		Multimap<Integer, IndexNodeDescriptor> returnMultimap = ArrayListMultimap.create();
		
		for(Integer partitionId : partitionIds){
			try {
				Collection<ServiceInstance<IndexPartitionStatus>> partitions = serviceDiscovery.queryForInstances("partition_" + partitionId);
				for(ServiceInstance<IndexPartitionStatus> si : partitions){
					IndexNodeDescriptor nodeDescriptor = new RemoteIndexNodeDescriptor(si.getAddress(), si.getPort());
					returnMultimap.put(partitionId, nodeDescriptor);					
				}
			} catch (Exception e) {
				throw new PartitionResolverException("Could not retrieve replica list of partition " + partitionId, e);
			}
		}
		
		return returnMultimap;
	}

}
