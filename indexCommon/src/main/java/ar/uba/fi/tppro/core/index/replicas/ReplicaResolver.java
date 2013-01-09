package ar.uba.fi.tppro.core.index.replicas;

import java.util.List;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

public abstract class ReplicaResolver {
	
	public abstract List<IndexNodeDescriptor> getReplicasForPartition(int partitionId);
	
	public Multimap<Integer, IndexNodeDescriptor> getReplicasForPartitions(List<Integer> partitionIds){
		
		Multimap<Integer, IndexNodeDescriptor> nodeMultimap = LinkedListMultimap.create();
		
		for(int partitionId : partitionIds){
			nodeMultimap.putAll(partitionId, getReplicasForPartition(partitionId));
		}
		
		return nodeMultimap;
	}

}
