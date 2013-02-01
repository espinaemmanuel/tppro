package ar.uba.fi.tppro.core.index;

public class StaticSocketPartitionResolver extends AbstractPartitionResolver {
	
	
	public StaticSocketPartitionResolver(RemoteNodePool nodePool) {
		super(nodePool);
	}

	public void addReplica(String host, int port, int pId){
		addSocketDescription(host, port, pId);
	}

	
}
