package ar.uba.fi.tppro.core.service;

import java.io.File;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class IndexCore implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);

	protected int port;
	protected File dataDir;

	protected IndexCoreHandler handler;
	protected IndexNode.Processor<IndexNode.Iface> processor;

	protected TServer server;

	public static void main(String[] args) {

		String port = System.getProperty("port", "9090");
		String dataDir = System.getProperty("dataDir", "data");

		new Thread(new IndexCore(Integer.parseInt(port), new File(dataDir)))
				.start();
	}

	public IndexCore(int port, File dataDir) {
		this.port = port;
		this.dataDir = dataDir;
	}

	public void stop() {
		logger.info("INDEX CORE: Stoping server");
		if (server != null) {
			server.stop();
		}
	}

	@Override
	public void run() {
		try {

			String initialPartitions = System.getProperty("initialPartitions");

			List<Integer> partitions = Lists.newArrayList();

			if (initialPartitions != null && !initialPartitions.isEmpty()) {
				for (String partitionStr : StringUtils.split(initialPartitions,
						',')) {
					int partId = Integer.parseInt(partitionStr);
					partitions.add(partId);
				}
			}

			handler = new IndexCoreHandler(this.dataDir);
			processor = new IndexNode.Processor<IndexNode.Iface>(handler);

			for (Integer partId : partitions) {
				if (!handler.containsPartition(partId)) {
					handler.createPartition(partId);
				}
			}

			Server webServer = new Server(8080);
			ResourceHandler resource_handler = new ResourceHandler();
			resource_handler.setDirectoriesListed(true);
			resource_handler.setWelcomeFiles(new String[] { "index.html" });

			resource_handler.setResourceBase(".");

			HandlerList handlers = new HandlerList();
			handlers.setHandlers(new Handler[] { resource_handler,
					new DefaultHandler() });
			webServer.setHandler(handlers);

			webServer.start();
			webServer.join();

			TServerTransport serverTransport = new TServerSocket(this.port);
			server = new TThreadPoolServer(new TThreadPoolServer.Args(
					serverTransport).processor(processor));

			logger.info("Starting the Index server on port " + this.port
					+ "...");
			server.serve();

		} catch (Exception e) {
			// TODO: mejorar esto
			e.printStackTrace();
		}
	}
}
