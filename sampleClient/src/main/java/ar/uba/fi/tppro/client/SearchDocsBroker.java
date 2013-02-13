package ar.uba.fi.tppro.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;
import ar.uba.fi.tppro.core.service.thrift.ParseException;

import com.google.common.collect.Lists;

public class SearchDocsBroker extends SearchDocsBase {

	@Override
	public void run(String[] args) throws IOException,
			NonExistentPartitionException, TException {
		String host = System.getProperty("host", "localhost");
		String port = System.getProperty("port", "9090");		
		String partitions = System.getProperty("partitions", "1,2,3");
		
		String[] partitionsStr = StringUtils.split(partitions, ',');
		
		List<Integer> partitionIds = Lists.newArrayList();
		
		for(String partitionStr : partitionsStr){
			partitionIds.add(Integer.parseInt(partitionStr));
		}

		TTransport transport = new TSocket(host, Integer.parseInt(port));
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexBroker.Client client = new IndexBroker.Client(protocol);
		transport.open();

		String curLine = ""; // Line read from standard in

		System.out.println("Enter a query (type 'quit' to exit): ");
		InputStreamReader converter = new InputStreamReader(System.in);
		BufferedReader in = new BufferedReader(converter);

		while (!(curLine.equals("quit"))) {
			curLine = in.readLine();

			if (!(curLine.equals("quit"))) {
				try {
					ParalellSearchResult result = client.search(partitionIds, curLine, 10,
							0);
					
					for(ar.uba.fi.tppro.core.service.thrift.Error error : result.errors){
						logger.error(error.code + " - " + error.desc);
					}
					
					if(result.errors.size() > 0){
						continue;
					}

					System.out.println("Num Found: " + result.qr.totalHits);
					System.out.println("Parsed Query: " + result.qr.parsedQuery);

					for (Hit hit : result.qr.hits) {
						System.out.println(hit.doc.fields);
					}
				} catch (ParseException e) {
					System.out.println("Parse error");
				}
			}
		}
	}

}
