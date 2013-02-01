package ar.uba.fi.tppro.core.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import ar.uba.fi.tppro.core.index.PartitionStatusObserver;
import ar.uba.fi.tppro.core.index.RemoteNodePool;
import ar.uba.fi.tppro.core.index.StaticSocketPartitionResolver;
import ar.uba.fi.tppro.core.index.TestUtil;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.lock.LockManager.LockType;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ReplicationTest {
	
	@Rule
	public TemporaryFolder testFolder = new TemporaryFolder();


	@Test
	public void testReplication() throws IOException, NonExistentPartitionException, TException, InterruptedException, LockAquireTimeoutException {
		File data1 = testFolder.newFolder();
		File data2 = testFolder.newFolder();

		// create index cores
		IndexCore core1 = new IndexCore(9000, data1);
		IndexCore core2 = new IndexCore(9090, data2);
		
		LockManager lockManager = mock(LockManager.class);
		IndexLock indexLock = mock(IndexLock.class);
		when(lockManager.aquire(eq(LockType.ADD), anyInt())).thenReturn(indexLock);
		core2.getHandler().setLockManager(lockManager);
		
		RemoteNodePool nodePool = new RemoteNodePool();
		StaticSocketPartitionResolver resolver = new StaticSocketPartitionResolver(nodePool);
		resolver.addReplica("localhost", 9000, 1);
		core2.getHandler().setPartitionResolver(resolver);
		
		PartitionStatusObserver statusObserver = mock(PartitionStatusObserver.class);
		core2.getHandler().setPartitionObserver(statusObserver);
		
		new Thread(core1).start();
		new Thread(core2).start();
		
		Thread.sleep(1000);

		IndexNode.Iface node1 = TestUtil.getCoreClient("localhost", 9000);
		IndexNode.Iface node2 = TestUtil.getCoreClient("localhost", 9090);
		
			
		// index some files
		List<Document> documents = TestUtil.createDocuments(this.getClass().getResource("movies.json").getPath());
		
		int numDocs = documents.size();
		
		node1.createPartition(1);
		node1.index(1, documents);
		
		// replicate
		node2.replicate(1);
		
		for(int i = 0; i < 10; i ++){
			Thread.sleep(5000);
			String status = node2.partitionStatus(1).status;
			if(status.equals("READY")){
				break;
			}
		}
		
		// search and check
		QueryResult result = node2.search(1, "*:*", 10, 0);
		
		assertEquals(numDocs, result.totalHits);
	}

}
