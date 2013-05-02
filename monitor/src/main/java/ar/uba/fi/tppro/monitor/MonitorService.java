package ar.uba.fi.tppro.monitor;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;

import ar.uba.fi.tppro.core.service.thrift.Monitor;

public class MonitorService implements Runnable {

	final Logger logger = LoggerFactory.getLogger(Monitor.class);

	protected int port;

	protected MonitorHandler handler;
	protected Monitor.Processor<Monitor.Iface> processor;

	protected static CuratorFramework curator;

	public static void main(String[] args) {

		String port = System.getProperty("port", "9000");
		new Thread(new MonitorService(Integer.parseInt(port))).start();
	}

	public MonitorService(int port) {
		this.port = port;
	}

	private static CuratorFramework createZookeeperClient(String zookeeperHost) {
		return CuratorFrameworkFactory.newClient(zookeeperHost,
				new RetryOneTime(1));
	}

	public void run() {

		String zookeeperHost = System
				.getProperty("zookeeper", "localhost:2181");
		if (zookeeperHost == null) {
			System.out.println("Zookeeper server not specified");
			return;
		}

		curator = createZookeeperClient(zookeeperHost);
		curator.start();

		handler = new MonitorHandler(curator);

		processor = new Monitor.Processor<Monitor.Iface>(handler);

		try {
			TServerTransport serverTransport = new TServerSocket(this.port);

			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(
					serverTransport).processor(processor));

			logger.info("Starting Monitor on port " + this.port + "...");
			server.serve();
		} catch (TTransportException e) {
			logger.error("cannot start the monitor", e);
		}
	}
}
