package ar.uba.fi.tppro.partition;

import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;

public class LocalPartitionResolver implements PartitionResolver {
	
	private Map<Integer, Multimap<Integer, IndexNodeDescriptor>> groups = Maps.newHashMap();

	@Override
	public Multimap<Integer, IndexNodeDescriptor> resolve(int shardId)
			throws PartitionResolverException {
		
		if(!groups.containsKey(shardId)){
			return LinkedListMultimap.create();
		}
		
		return groups.get(shardId);
	}

	@Override
	public void registerPartition(int shardId, int partitionId,
			IndexNodeDescriptor descriptor, IndexPartitionStatus status)
			throws PartitionResolverException {
		
		Multimap<Integer, IndexNodeDescriptor> partitionsMap = groups.get(shardId);
		if(partitionsMap == null){
			partitionsMap = HashMultimap.create();
			
			groups.put(shardId, partitionsMap);
		}
		
		partitionsMap.put(partitionId, descriptor);		
	}

	@Override
	public void updatePartitionStatus(int shardId, int partitionId,
			IndexNodeDescriptor descriptor, IndexPartitionStatus status)
			throws PartitionResolverException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<PartitionDescriptor> getAll() throws PartitionResolverException {
		// TODO Auto-generated method stub
		return null;
	}
	

	
}
