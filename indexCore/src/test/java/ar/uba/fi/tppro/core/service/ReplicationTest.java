package ar.uba.fi.tppro.core.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;

import ar.uba.fi.tppro.core.index.RemoteNodePool;
import ar.uba.fi.tppro.core.index.TestUtil;
import ar.uba.fi.tppro.core.index.ClusterManager.ClusterManager;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.ZkShardVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.MessageId;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.partition.StaticSocketPartitionResolver;

public class ReplicationTest extends IndexCoreTest {
	
	@Test
	public void testReplication() throws Exception {
		TestingServer server = new TestingServer();

		try {
			CuratorFramework client = CuratorFrameworkFactory.newClient(
					server.getConnectString(), new RetryOneTime(1));
			client.start();
			
			this.versionTracker = new ZkShardVersionTracker(client);

			this.lockManager = mock(LockManager.class);
			IndexLock indexLock = mock(IndexLock.class);
			when(lockManager.aquire(eq(1), anyInt())).thenReturn(
					indexLock);

			RemoteNodePool nodePool = new RemoteNodePool();
			this.partitionResolver = new StaticSocketPartitionResolver(
					nodePool);
			partitionResolver.addReplica("localhost", 9000, 1, 1);
			
			clusterManager = mock(ClusterManager.class);

			IndexServer core1 = createIndexServer(client, 9000);
			IndexServer core2 = createIndexServer(client, 9090);
			
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
			node1.prepareCommit(1, 1, new MessageId(0,  1), documents);
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
