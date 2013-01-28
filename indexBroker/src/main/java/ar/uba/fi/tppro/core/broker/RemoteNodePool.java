package ar.uba.fi.tppro.core.broker;

import java.util.Map;

import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;

public class RemoteNodePool {
	
	private Map<SocketDescription, IndexNodeDescriptor> sockets = Maps.newHashMap();

	public IndexNodeDescriptor getIndexNodeDescriptor(SocketDescription sd) {
	//	if(!sockets.containsKey(sd)){
			RemoteIndexNodeDescriptor descriptor = new RemoteIndexNodeDescriptor(sd.host, sd.port);
	//		sockets.put(sd, descriptor);
	//	}

	//	return sockets.get(sd);
			
			return descriptor;
	}

}
