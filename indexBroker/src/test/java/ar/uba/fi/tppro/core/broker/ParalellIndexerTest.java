package ar.uba.fi.tppro.core.broker;

import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.thrift.TException;
import org.junit.Test;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.StaleVersionException;
import ar.uba.fi.tppro.core.index.versionTracker.VersionTrackerServerException;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.MessageId;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;

public class ParalellIndexerTest {
	
	
	protected List<Document> sampleDocs(){
		
		List<Document> documents = Lists.newArrayList();
		
		for(int i=0; i < 10 ; i++){
			Document d = new Document();
			d.fields = Maps.newHashMap();
			d.fields.put("order", Integer.toString(i));
			
			documents.add(d);
		}
		
		return documents;
	}
	
	protected IndexNodeDescriptor buildDescriptor(IndexNode.Iface client) throws IndexNodeDescriptorException{
		IndexNodeDescriptor descriptor = mock(IndexNodeDescriptor.class);
		when(descriptor.getClient()).thenReturn(client);
		
		return descriptor;
	}

	@Test
	public void distributeAndIndexTest() throws LockAquireTimeoutException, NonExistentPartitionException, TException, IndexNodeDescriptorException, VersionTrackerServerException, StaleVersionException {
		
		IndexNode.Iface p1_r1 = mock(IndexNode.Iface.class);
		IndexNode.Iface p1_r2 = mock(IndexNode.Iface.class);
		IndexNode.Iface p1_r3 = mock(IndexNode.Iface.class);
		IndexNode.Iface p2_r1 = mock(IndexNode.Iface.class);
		IndexNode.Iface p2_r2 = mock(IndexNode.Iface.class);
		IndexNode.Iface p2_r3 = mock(IndexNode.Iface.class);
		
		Multimap<Integer, IndexNodeDescriptor> partitions = LinkedListMultimap.create();
		partitions.put(1, buildDescriptor(p1_r1));
		partitions.put(1, buildDescriptor(p1_r2));
		partitions.put(1, buildDescriptor(p1_r3));
		partitions.put(2, buildDescriptor(p2_r1));
		partitions.put(2, buildDescriptor(p2_r2));
		partitions.put(2, buildDescriptor(p2_r3));
		
		LockManager lockManager = mock(LockManager.class);
		IndexLock indexLock = mock(IndexLock.class);
		when(lockManager.aquire(eq(1), anyInt())).thenReturn(indexLock);
		
		GroupVersionTracker versionTracker = mock(GroupVersionTracker.class);
		
		VersionGenerator mockVersionGenerator = mock(VersionGenerator.class);
		when(mockVersionGenerator.getNextVersion()).thenReturn(1234l);

		ParalellIndexer indexer = new ParalellIndexer(lockManager, versionTracker, mockVersionGenerator);
		
		List<Document> documents = sampleDocs();
		indexer.distributeAndIndex(1, partitions, documents);
		
		verify(p1_r1).prepareCommit(1, 1, new MessageId(0, 1234l), Lists.newArrayList(documents.get(0), documents.get(2), documents.get(4), documents.get(6), documents.get(8)));
		verify(p1_r2).prepareCommit(1, 1, new MessageId(0, 1234l), Lists.newArrayList(documents.get(0), documents.get(2), documents.get(4), documents.get(6), documents.get(8)));
		verify(p1_r3).prepareCommit(1, 1, new MessageId(0, 1234l), Lists.newArrayList(documents.get(0), documents.get(2), documents.get(4), documents.get(6), documents.get(8)));
		verify(p2_r1).prepareCommit(1, 2, new MessageId(0, 1234l), Lists.newArrayList(documents.get(1), documents.get(3), documents.get(5), documents.get(7), documents.get(9)));
		verify(p2_r2).prepareCommit(1, 2, new MessageId(0, 1234l), Lists.newArrayList(documents.get(1), documents.get(3), documents.get(5), documents.get(7), documents.get(9)));
		verify(p2_r3).prepareCommit(1, 2, new MessageId(0, 1234l), Lists.newArrayList(documents.get(1), documents.get(3), documents.get(5), documents.get(7), documents.get(9)));
		
		verifyNoMoreInteractions(p1_r1);
		verifyNoMoreInteractions(p1_r2);
		verifyNoMoreInteractions(p1_r3);
		verifyNoMoreInteractions(p2_r1);
		verifyNoMoreInteractions(p2_r2);
		verifyNoMoreInteractions(p2_r3);

	}

}
