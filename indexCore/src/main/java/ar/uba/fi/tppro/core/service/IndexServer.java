package ar.uba.fi.tppro.core.service;

import java.io.File;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Collection;
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
import com.netflix.curator.x.discovery.ServiceInstanceBuilder;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionsGroup;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.util.NetworkUtils;

public class IndexServer implements Runnable {

	final Logger logger = LoggerFactory.getLogger(IndexPartitionsGroup.class);

	protected int port;
	protected File dataDir;

	protected IndexPartitionsGroup handler;

	protected IndexNode.Processor<IndexNode.Iface> processor;

	protected TServer server;

	public static void main(String[] args) throws NumberFormatException,
			Exception {

		String port = System.getProperty("port", "9090");
		String dataDir = System.getProperty("dataDir", "data");

		new Thread(new IndexServer(Integer.parseInt(port), new File(dataDir)))
				.start();

	}

	public IndexServer(int port, File dataDir) throws Exception {
		this.port = port;
		this.dataDir = dataDir;

		Collection<InetAddress> ips = NetworkUtils.getAllLocalIPs();
		if (ips.size() == 0) {
			throw new Exception("Could not retrieve the local ip");
		}
		String address = ips.iterator().next().getHostAddress();
		IndexNodeDescriptor localNodeDescriptor = new RemoteIndexNodeDescriptor(
				address, port);
		
		
		this.handler = new IndexPartitionsGroup(localNodeDescriptor, null, null);
		this.handler.open(this.dataDir);
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

			if (!this.dataDir.exists()) {
				this.dataDir.mkdir();
			}

			processor = new IndexNode.Processor<IndexNode.Iface>(handler);

			for (Integer partId : partitions) {
				if (!handler.containsPartition(partId)) {
					handler.createPartition(partId);
				}
			}

			Server webServer = new Server(this.port + 1);
			ResourceHandler resource_handler = new ResourceHandler();
			resource_handler.setDirectoriesListed(true);
			resource_handler.setWelcomeFiles(new String[] { "index.html" });
			resource_handler.setResourceBase(this.dataDir.getAbsolutePath());

			HandlerList handlers = new HandlerList();
			handlers.setHandlers(new Handler[] { resource_handler,
					new DefaultHandler() });
			webServer.setHandler(handlers);

			logger.info("Starting http server on port port " + (this.port + 1)
					+ "...");
			webServer.start();

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

	public IndexPartitionsGroup getHandler() {
		return handler;
	}
}
