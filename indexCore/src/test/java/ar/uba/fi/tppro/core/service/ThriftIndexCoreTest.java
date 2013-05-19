package ar.uba.fi.tppro.core.service;
import static org.junit.Assert.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.curator.framework.CuratorFramework;

import ar.uba.fi.tppro.core.index.ClusterManager.ClusterManager;
import ar.uba.fi.tppro.core.index.lock.NullLockManager;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.core.service.IndexServer;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.MessageId;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.partition.StaticSocketPartitionResolver;

public class ThriftIndexCoreTest extends IndexCoreTest{

	private static final int PORT = 7911;

	@Before
	public void startServer() throws Exception {
		// Start thrift server in a seperate thread
		
		this.versionTracker = mock(GroupVersionTracker.class);
		when(versionTracker.getCurrentVersion(123)).thenReturn(0l);
		
		this.partitionResolver = mock(StaticSocketPartitionResolver.class);
		this.lockManager = new NullLockManager();
		CuratorFramework client = mock(CuratorFramework.class);
		this.clusterManager = mock(ClusterManager.class);
		
		IndexServer server = this.createIndexServer(client, PORT);
		new Thread(server).start();
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

		if(!client.containsPartition(1, 123)){
			client.createPartition(1, 123);
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
		
		if(!client.containsPartition(1, 123)){
			client.createPartition(1, 123);
		}
		
		client.prepareCommit(1, 123, new MessageId(0, 1), Lists.newArrayList(doc));
		client.commit(1, 123);
		QueryResult queryResult = client.search(1, 123, "information", 10, 0);
		
		assertEquals(1, queryResult.totalHits);
		assertEquals(1, queryResult.hits.size());
		assertEquals(docTitle, queryResult.hits.get(0).doc.fields.get("title"));
		assertEquals(docText, queryResult.hits.get(0).doc.fields.get("text"));

		transport.close();
	}

}
