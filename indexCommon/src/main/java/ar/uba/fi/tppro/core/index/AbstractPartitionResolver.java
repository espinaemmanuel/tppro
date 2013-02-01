package ar.uba.fi.tppro.core.index;

import java.util.Collection;
import java.util.List;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public abstract class AbstractPartitionResolver implements PartitionResolver {
	
	private RemoteNodePool remoteNodePool;
	private Multimap<Integer, SocketDescription> socketDescMap = LinkedListMultimap.create();
	
	public AbstractPartitionResolver(RemoteNodePool nodePool) {
		this.remoteNodePool = nodePool;
	}

	@Override
	public Multimap<Integer, IndexNodeDescriptor> resolve(
			List<Integer> partitionIds) {
		
		Multimap<Integer, IndexNodeDescriptor> returnMap = LinkedListMultimap.create();
		
		for(Integer pId : partitionIds){
			Collection<SocketDescription> socketDescList = socketDescMap.get(pId);
			
			if(socketDescList != null && socketDescList.size() != 0){
				for(SocketDescription sd : socketDescList){
					returnMap.put(pId, remoteNodePool.getIndexNodeDescriptor(sd));
				}
			}	
		}
		
		return returnMap;
	}
	
	protected void addSocketDescription(String host, int port, int pId){
		SocketDescription socketDesc = new SocketDescription();
		socketDesc.host = host;
		socketDesc.port = port;
		
		socketDescMap.put(pId, socketDesc);
	}


}
