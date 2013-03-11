package ar.uba.fi.tppro.core.index;

import ar.uba.fi.tppro.core.index.httpClient.PartitionHttpClient;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public interface IndexNodeDescriptor {
	
	public IndexNode.Iface getClient() throws IndexNodeDescriptorException;
	
	public PartitionHttpClient getHttpClient(int groupId, int pId);
	
	public String getHost();
	
	public int getPort();
	
	public void close();
}
