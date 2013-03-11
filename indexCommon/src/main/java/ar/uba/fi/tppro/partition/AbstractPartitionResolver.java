package ar.uba.fi.tppro.partition;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.RemoteNodePool;
import ar.uba.fi.tppro.core.index.SocketDescription;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

public abstract class AbstractPartitionResolver implements PartitionResolver {
	
	private RemoteNodePool remoteNodePool;
	private Table<Integer, Integer, List<SocketDescription>> socketDescTable = HashBasedTable.create();
	
	public AbstractPartitionResolver(RemoteNodePool nodePool) {
		this.remoteNodePool = nodePool;
	}

	@Override
	public Multimap<Integer, IndexNodeDescriptor> resolve(int shardId) {
		
		Multimap<Integer, IndexNodeDescriptor> returnMap = LinkedListMultimap.create();
		Map<Integer, List<SocketDescription>> partitionMap = socketDescTable.row(shardId);
		
		for(Integer pId : partitionMap.keySet()){
			Collection<SocketDescription> socketDescList = partitionMap.get(pId);
			
			if(socketDescList != null && socketDescList.size() != 0){
				for(SocketDescription sd : socketDescList){
					returnMap.put(pId, remoteNodePool.getIndexNodeDescriptor(sd));
				}
			}	
		}
		
		return returnMap;
	}
	
	protected void addSocketDescription(String host, int port, int sId, int pId){
		SocketDescription socketDesc = new SocketDescription();
		socketDesc.host = host;
		socketDesc.port = port;
		
		List<SocketDescription> descriptionList = socketDescTable.get(sId, pId);
		if(descriptionList == null){
			descriptionList = Lists.newArrayList();
			socketDescTable.put(sId, pId, descriptionList);
		}
		
		descriptionList.add(socketDesc);
	}


}
