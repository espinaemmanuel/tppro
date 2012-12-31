package ar.uba.fi.tppro.core.service;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.service.ThriftIndexCore;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ThriftIndexCoreTest {

	private static final int PORT = 7911;

	@BeforeClass
	public static void startServer() throws URISyntaxException, IOException {
		// Start thrift server in a seperate thread
		new Thread(new ThriftIndexCore(PORT, new File("dataDir"))).start();
		try {
			// wait for the server start up
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testConnection() throws TTransportException, TException {
		TTransport transport = new TSocket("localhost", PORT);
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();

		if(!client.containsPartition(123)){
			client.createPartition(123);
		}

		transport.close();
	}
	
	
	@Test
	public void indexTest() throws TTransportException, TException {
		TTransport transport = new TSocket("localhost", PORT);
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();
		
		Document doc = new Document();
		doc.fields = Maps.newHashMap();
		
		String docTitle = "Apache Lucene";
		String docText = "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.";
		doc.fields.put("title", docTitle);
		doc.fields.put("text", docText);
		
		if(!client.containsPartition(123)){
			client.createPartition(123);
		}
		
		client.index(123, Lists.newArrayList(doc));		
		QueryResult queryResult = client.search(123, "information", 10, 0);
		
		assertEquals(2, queryResult.totalHits);
		assertEquals(1, queryResult.hits.size());
		assertEquals(docTitle, queryResult.hits.get(0).doc.fields.get("title"));
		assertEquals(docText, queryResult.hits.get(0).doc.fields.get("text"));

		transport.close();
	}

}
