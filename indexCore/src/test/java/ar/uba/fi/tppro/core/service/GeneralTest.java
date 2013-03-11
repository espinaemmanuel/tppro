package ar.uba.fi.tppro.core.service;

import static org.junit.Assert.*;

import org.apache.thrift.TException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionsGroup;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.lock.NullLockManager;
import ar.uba.fi.tppro.core.index.versionTracker.ShardVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.VersionTrackerServerException;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.partition.PartitionResolver;

public class GeneralTest {
	
	final Logger logger = LoggerFactory.getLogger(GeneralTest.class);
	
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

	@Test
	public void testIndex() throws TException, VersionTrackerServerException {
		IndexNodeDescriptor localNodeDescriptor = new RemoteIndexNodeDescriptor("localhost", 1234);
		PartitionResolver partitionResolver = mock(PartitionResolver.class);
		ShardVersionTracker versionTracker = mock(ShardVersionTracker.class);
		when(versionTracker.getCurrentVersion(123)).thenReturn(0);
		LockManager lockManager = new NullLockManager();
		
		IndexPartitionsGroup partitionsGroup = new IndexPartitionsGroup(localNodeDescriptor, partitionResolver, versionTracker, lockManager);
		partitionsGroup.open(testFolder.getRoot());
		
		logger.info("Creating index in directory " + testFolder.getRoot());
		
		Document doc = new Document();
		doc.fields = Maps.newHashMap();
		
		String docTitle = "Apache Lucene";
		String docText = "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.";
		doc.fields.put("title", docTitle);
		doc.fields.put("text", docText);
		
		if(!partitionsGroup.containsPartition(1, 123)){
			partitionsGroup.createPartition(1, 123);
		}
		
		partitionsGroup.prepareCommit(1, 123, 1, Lists.newArrayList(doc));
		partitionsGroup.commit(1, 123);

		QueryResult queryResult = partitionsGroup.search(1, 123, "information", 10, 0);
		
		assertEquals(1, queryResult.totalHits);
		assertEquals(1, queryResult.hits.size());
		assertEquals(docTitle, queryResult.hits.get(0).doc.fields.get("title"));
		assertEquals(docText, queryResult.hits.get(0).doc.fields.get("text"));
	}

}
