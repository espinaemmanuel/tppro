package ar.uba.fi.tppro.stress;

import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.index.RemoteBrokerNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;

public class QueryExecutor implements Runnable {

	private List<String> queries;
	private String brokerHost;
	private int port;

	public QueryExecutor(List<String> queries, String brokerHost, int port) {
		super();
		this.queries = queries;
		this.brokerHost = brokerHost;
		this.port = port;
	}

	public void run() {

		try {

			RemoteBrokerNodeDescriptor remoteNode = new RemoteBrokerNodeDescriptor(
					brokerHost, port);
			IndexBroker.Iface broker = remoteNode.getClient();

			Random random = new Random();

			while (true) {

				String query = queries.get(random.nextInt(queries.size()));

				long t1 = System.currentTimeMillis();
				ParalellSearchResult result = broker.search(1, query, 1000, 0);
				long t2 = System.currentTimeMillis();

				int totalHits = result.qr.totalHits;
				long elapsedTime = t2 - t1;

				System.out.println(String.format("time:%d,results:%d",
						elapsedTime, totalHits));
			}

		} catch (IndexNodeDescriptorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParalellSearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NonExistentPartitionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
