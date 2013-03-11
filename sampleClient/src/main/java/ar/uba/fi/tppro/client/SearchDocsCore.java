package ar.uba.fi.tppro.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.IndexNode.Client;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class SearchDocsCore extends SearchDocsBase {

	@Override
	public void run(String[] args) throws IOException,
			NonExistentPartitionException, TException {
		String host = System.getProperty("host", "localhost");
		String port = System.getProperty("port", "9090");

		TTransport transport = new TSocket(host, Integer.parseInt(port));
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();

		String curLine = ""; // Line read from standard in

		System.out.println("Enter a query (type 'quit' to exit): ");
		InputStreamReader converter = new InputStreamReader(System.in);
		BufferedReader in = new BufferedReader(converter);

		while (!(curLine.equals("quit"))) {
			curLine = in.readLine();

			if (!(curLine.equals("quit"))) {
				try {
					QueryResult result = client.search(1, 0, curLine, 10,
							0);

					System.out.println("Num Found: " + result.totalHits);
					System.out.println("Parsed Query: " + result.parsedQuery);

					for (Hit hit : result.hits) {
						System.out.println(hit.doc.fields);
					}
				} catch (ParseException e) {
					System.out.println("Parse error");
				}
			}
		}
	}

}
