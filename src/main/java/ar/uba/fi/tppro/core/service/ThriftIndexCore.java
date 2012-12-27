package ar.uba.fi.tppro.core.service;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class ThriftIndexCore implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);
	
	protected int port;

	protected IndexCoreHandler handler;
	protected IndexNode.Processor<IndexNode.Iface> processor;

	public static void main(String[] args) {
		//TODO setear el puerto por propiedad
		new Thread(new ThriftIndexCore(9090)).start();
	}
	
	public ThriftIndexCore(int port){
		this.port = port;
	}

	@Override
	public void run() {
		try {
			handler = new IndexCoreHandler();
			processor = new IndexNode.Processor<IndexNode.Iface>(handler);
			
			TServerTransport serverTransport = new TServerSocket(this.port);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(
					serverTransport).processor(processor));
			
			logger.info("Starting the Index server...");
			server.serve();

		} catch (Exception e) {
			// TODO: mejorar esto
			e.printStackTrace();
		}

	}
}
