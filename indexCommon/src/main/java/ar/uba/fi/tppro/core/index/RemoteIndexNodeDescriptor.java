package ar.uba.fi.tppro.core.index;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import ar.uba.fi.tppro.core.index.httpClient.PartitionHttpClient;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class RemoteIndexNodeDescriptor implements IndexNodeDescriptor {
	
	private TTransport transport;
	private TProtocol protocol;
	private String host;
	private int port;
	private IndexNode.Iface client;

	public RemoteIndexNodeDescriptor(String host, int port) {
		super();
		this.transport = new TSocket(host, port);
		this.protocol = new TBinaryProtocol(transport);
		this.host = host;
		this.port = port;
	}
	
	synchronized public void open() throws TTransportException{
		transport.open();
	}
	
	synchronized public void close() {
		if(transport.isOpen()){
			transport.close();
		}
		
		this.client = null;
	}
	
	@Override
	synchronized public IndexNode.Iface getClient() throws IndexNodeDescriptorException{
		if(this.client == null){
			if (!transport.isOpen()) {
				try {
					transport.open();
				} catch (TTransportException e) {
					throw new IndexNodeDescriptorException("Could not open socket to remote host", e);
				}
			}
			
			this.client = new SynchronizedClient(new IndexNode.Client(protocol));
		}

		return this.client;
	}
	
	@Override
	public String toString(){
		return String.format("Remote Node: %s:%d", this.host, this.port);
	}

	@Override
	public PartitionHttpClient getHttpClient(int groupId, int partitionId) {
		return new PartitionHttpClient(host, port + 1, groupId, partitionId);
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
