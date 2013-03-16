package ar.uba.fi.tppro.core.index;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import ar.uba.fi.tppro.core.service.thrift.IndexBroker;

public class RemoteBrokerNodeDescriptor implements BrokerNodeDescriptor {
	
	private TTransport transport;
	TProtocol protocol;
	String host;
	int port;

	public RemoteBrokerNodeDescriptor(String host, int port) {
		super();
		this.transport = new TSocket(host, port);
		this.protocol = new TBinaryProtocol(transport);
		this.host = host;
		this.port = port;
	}
	
	public void open() throws TTransportException{
		transport.open();
	}
	
	public void close() {
		if(transport.isOpen()){
			transport.close();
		}
	}
	
	@Override
	public IndexBroker.Iface getClient() throws IndexNodeDescriptorException{
		if (!transport.isOpen()) {
			try {
				transport.open();
			} catch (TTransportException e) {
				throw new IndexNodeDescriptorException("Could not open socket to remote host", e);
			}
		}
		return new IndexBroker.Client(protocol);
	}
	
	@Override
	public String toString(){
		return String.format("Remote Node: %s:%d", this.host, this.port);
	}

	@Override
	public String getHost() {
		return this.host;
	}

	@Override
	public int getPort() {
		return this.port;
	}
	

}
