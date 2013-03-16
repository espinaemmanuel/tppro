package ar.uba.fi.tppro.core.index;

import ar.uba.fi.tppro.core.service.thrift.IndexBroker;

public interface BrokerNodeDescriptor {
	
	public IndexBroker.Iface getClient() throws IndexNodeDescriptorException;
		
	public String getHost();
	
	public int getPort();
	
	public void close();
}
