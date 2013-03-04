package ar.uba.fi.tppro.partition;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;
import ar.uba.fi.tppro.core.index.RemoteNodePool;

public class StaticSocketPartitionResolver extends AbstractPartitionResolver {
	
	
	public StaticSocketPartitionResolver(RemoteNodePool nodePool) {
		super(nodePool);
	}

	public void addReplica(String host, int port, int pId){
		addSocketDescription(host, port, pId);
	}

	@Override
	public void registerPartition(int partitionId,
			IndexNodeDescriptor descriptor, IndexPartitionStatus status)
			throws PartitionResolverException {
		// TODO Auto-generated method stub
		
	}

	
}
