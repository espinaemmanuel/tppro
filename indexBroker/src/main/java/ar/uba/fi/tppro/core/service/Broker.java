package ar.uba.fi.tppro.core.service;

import java.io.File;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.broker.IndexBrokerHandler;
import ar.uba.fi.tppro.core.broker.NullLockManager;
import ar.uba.fi.tppro.core.index.RemoteNodePool;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.partition.CSVPartitionResolver;

public class Broker implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexBroker.class);
	
	protected int port;
	protected File dataDir;

	protected IndexBrokerHandler handler;
	protected IndexBroker.Processor<IndexBroker.Iface> processor;

	public static void main(String[] args) {
		
		String port = System.getProperty("port", "9090");
		String dataDir = System.getProperty("dataDir", "data");

		new Thread(new Broker(Integer.parseInt(port), new File(dataDir))).start();
	}
	
	public Broker(int port, File dataDir){
		this.port = port;
		this.dataDir = dataDir;
	}

	@Override
	public void run() {
		try {
			String partitions = System.getProperty("partitions", "partitions.csv");
			
			RemoteNodePool nodePool = new RemoteNodePool();
			CSVPartitionResolver partitionResolver = new CSVPartitionResolver(nodePool);
			
			File partitionsFile = new File(partitions);
			partitionResolver.load(partitionsFile);
			
			NullLockManager lockManager = new NullLockManager();
			
			handler = new IndexBrokerHandler(partitionResolver, lockManager);
			processor = new IndexBroker.Processor<IndexBroker.Iface>(handler);
			
			TServerTransport serverTransport = new TServerSocket(this.port);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(
					serverTransport).processor(processor));
			
			logger.info("Starting the Index server on port " + this.port + "...");
			server.serve();

		} catch (Exception e) {
			// TODO: mejorar esto
			e.printStackTrace();
		}

	}
}
