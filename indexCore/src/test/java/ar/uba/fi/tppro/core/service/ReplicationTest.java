package ar.uba.fi.tppro.core.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;

import ar.uba.fi.tppro.core.index.RemoteNodePool;
import ar.uba.fi.tppro.core.index.TestUtil;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.ShardVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.ZkShardVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.partition.StaticSocketPartitionResolver;

public class ReplicationTest {
	
	final Logger logger = LoggerFactory.getLogger(ReplicationTest.class);

	@Rule
	public TemporaryFolder testFolder = new TemporaryFolder();

	@Test
	public void testReplication() throws Exception {
		TestingServer server = new TestingServer();

		try {
			
			File data1 = testFolder.newFolder();
			File data2 = testFolder.newFolder();
			
			logger.debug("Temp dir 1: " + data1);
			logger.debug("Temp dir 2: " + data2);



			CuratorFramework client = CuratorFrameworkFactory.newClient(
					server.getConnectString(), new RetryOneTime(1));
			
			client.start();
			
			ShardVersionTracker versionTracker = new ZkShardVersionTracker(client);

			LockManager lockManager = mock(LockManager.class);
			IndexLock indexLock = mock(IndexLock.class);
			when(lockManager.aquire(eq(1), anyInt())).thenReturn(
					indexLock);

			RemoteNodePool nodePool = new RemoteNodePool();
			StaticSocketPartitionResolver partitionResolver = new StaticSocketPartitionResolver(
					nodePool);
			partitionResolver.addReplica("localhost", 9000, 1, 1);

			// create index cores
			IndexServer core1 = new IndexServer(9000, data1, partitionResolver, versionTracker, lockManager);
			IndexServer core2 = new IndexServer(9090, data2, partitionResolver, versionTracker, lockManager);

			new Thread(core1).start();
			new Thread(core2).start();

			Thread.sleep(1000);

			IndexNode.Iface node1 = TestUtil.getCoreClient("localhost", 9000);
			IndexNode.Iface node2 = TestUtil.getCoreClient("localhost", 9090);

			// index some files
			List<Document> documents = TestUtil.createDocuments(this.getClass()
					.getResource("movies.json").getPath());

			int numDocs = documents.size();

			node1.createPartition(1, 1);
			node1.prepareCommit(1, 1, 1, documents);
			node1.commit(1, 1);
			
			versionTracker.setShardVersion(1, 1);

			// replicate
			// node2.replicate(1);
			node2.createPartition(1, 1);

			for (int i = 0; i < 10; i++) {
				Thread.sleep(5000);
				String status = node2.partitionStatus(1, 1).status;
				if (status.equals("READY")) {
					break;
				}
			}

			// search and check
			QueryResult result = node2.search(1, 1, "*:*", 10, 0);

			assertEquals(numDocs, result.totalHits);
		} finally {
			server.close();
		}
	}

}
