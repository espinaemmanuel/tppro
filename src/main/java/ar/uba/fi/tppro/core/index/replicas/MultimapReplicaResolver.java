package ar.uba.fi.tppro.core.index.replicas;

import java.util.List;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

/**
 * Static replica resolver used for testing
 * 
 * @author emmanuel
 *
 */

public class MultimapReplicaResolver extends ReplicaResolver {
	
	private Multimap<Integer, IndexNodeDescriptor> multimap = LinkedListMultimap.create();

	@Override
	public List<IndexNodeDescriptor> getReplicasForPartition(int partitionId) {
		return Lists.newArrayList(multimap.get(partitionId));
	}
	
	public void add(int partitionId, IndexNodeDescriptor node){
		multimap.put(partitionId, node);
	}

}
