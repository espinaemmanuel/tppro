package ar.uba.fi.tppro.core.index;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class IndexNodeDescriptor {
	
	private String host;
	private int port;

	public IndexNodeDescriptor(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}
	
	public IndexNode.Client getClient(){
		TTransport transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport);
		return new IndexNode.Client(protocol);
	}
	

}
