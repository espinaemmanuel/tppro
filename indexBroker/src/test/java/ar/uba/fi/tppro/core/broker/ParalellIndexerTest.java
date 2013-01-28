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
import ar.uba.fi.tppro.core.index.lock.LockManager.LockType;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
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
	public void distributeAndIndexTest() throws LockAquireTimeoutException, NonExistentPartitionException, TException, IndexNodeDescriptorException {
		
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
		when(lockManager.aquire(eq(LockType.ADD), anyInt())).thenReturn(indexLock);
		
		ParalellIndexer indexer = new ParalellIndexer(lockManager);
		
		List<Document> documents = sampleDocs();
		indexer.distributeAndIndex(partitions, documents);
		
		verify(p1_r1).index(1, Lists.newArrayList(documents.get(0), documents.get(2), documents.get(4), documents.get(6), documents.get(8)));
		verify(p1_r2).index(1, Lists.newArrayList(documents.get(0), documents.get(2), documents.get(4), documents.get(6), documents.get(8)));
		verify(p1_r3).index(1, Lists.newArrayList(documents.get(0), documents.get(2), documents.get(4), documents.get(6), documents.get(8)));
		verify(p2_r1).index(2, Lists.newArrayList(documents.get(1), documents.get(3), documents.get(5), documents.get(7), documents.get(9)));
		verify(p2_r2).index(2, Lists.newArrayList(documents.get(1), documents.get(3), documents.get(5), documents.get(7), documents.get(9)));
		verify(p2_r3).index(2, Lists.newArrayList(documents.get(1), documents.get(3), documents.get(5), documents.get(7), documents.get(9)));
		
		verifyNoMoreInteractions(p1_r1);
		verifyNoMoreInteractions(p1_r2);
		verifyNoMoreInteractions(p1_r3);
		verifyNoMoreInteractions(p2_r1);
		verifyNoMoreInteractions(p2_r2);
		verifyNoMoreInteractions(p2_r3);

	}

}
