package ar.uba.fi.tppro.core.index;

import ar.uba.fi.tppro.core.index.httpClient.PartitionHttpClient;
import ar.uba.fi.tppro.core.service.thrift.IndexNode.Iface;

public class LocalNodeDescriptor implements IndexNodeDescriptor{
	
	private Iface localInterface;
	
	
	public void setlocalIndex(Iface localInterface){
		this.localInterface = localInterface;
	}

	@Override
	public Iface getClient() throws IndexNodeDescriptorException {
		return this.localInterface;
	}

	@Override
	public PartitionHttpClient getHttpClient(int groupId, int pId) {
		return null;
	}

	@Override
	public String getHost() {
		return null;
	}

	@Override
	public int getPort() {
		return 0;
	}

	@Override
	public void close() {
		
	}
	
	

}
