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
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			IndexNodeDescriptorException, ParalellSearchException,
			NonExistentPartitionException, TException, InterruptedException {

		if (args.length < 3) {
			System.out.println("USE: [-Dthreads=1] host port queriesFile");
			return;
		}

		int threads = Integer.parseInt(System.getProperty("threads", "1"));

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

		List<Thread> threadList = Lists.newArrayList();

		for (int i = 0; i < threads; i++) {
			threadList.add(new Thread(new QueryExecutor(queriesList,
					brokerHost, port)));
		}

		for (Thread t : threadList) {
			t.start();
		}

		for (Thread t : threadList) {
			t.wait();
		}
	}

}
