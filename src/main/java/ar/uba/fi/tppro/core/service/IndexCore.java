package ar.uba.fi.tppro.core.service;

import java.io.File;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class IndexCore implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);
	
	protected int port;
	protected File dataDir;

	protected IndexCoreHandler handler;
	protected IndexNode.Processor<IndexNode.Iface> processor;

	public static void main(String[] args) {
		//TODO setear el puerto por propiedad
		new Thread(new IndexCore(9090, new File("dataDir"))).start();
	}
	
	public IndexCore(int port, File dataDir){
		this.port = port;
		this.dataDir = dataDir;
	}

	@Override
	public void run() {
		try {
			handler = new IndexCoreHandler(this.dataDir);
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
