package ar.uba.fi.tppro.stress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.index.RemoteBrokerNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;

public class StressTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IndexNodeDescriptorException 
	 * @throws TException 
	 * @throws NonExistentPartitionException 
	 * @throws ParalellSearchException 
	 */
	public static void main(String[] args) throws IOException, IndexNodeDescriptorException, ParalellSearchException, NonExistentPartitionException, TException {

		if (args.length < 3) {
			System.out.println("USE: host port queriesFile");
			return;
		}

		String brokerHost = args[0];
		int port = Integer.parseInt(args[1]);
		File queries = new File(args[2]);

		BufferedReader reader = new BufferedReader(new FileReader(queries));

		List<String> queriesList = Lists.newArrayList();

		String line = reader.readLine();
		while (line != null) {
			queriesList.add(line);
			line = reader.readLine();
		}
		
		reader.close();

		RemoteBrokerNodeDescriptor remoteNode = new RemoteBrokerNodeDescriptor(
				brokerHost, port);
		IndexBroker.Iface broker = remoteNode.getClient();
		
		Random random = new Random();

		while (true) {
			
			String query = queriesList.get(random.nextInt(queriesList.size()));
			
			long t1 = System.currentTimeMillis();
			ParalellSearchResult result = broker.search(1, query, 1000, 0);
			long t2 = System.currentTimeMillis();
			
			int totalHits = result.qr.totalHits;
			long elapsedTime = t2 - t1;
			
			System.out.println(String.format("time:%d,results:%d", elapsedTime, totalHits));
		}
	}

}
