package ar.uba.fi.tppro.core.broker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.IndexSearcher;
import org.apache.thrift.TException;
import org.junit.Test;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ParalellSearcherTest {
	
	public Hit buildHit(double score, String ... fields){
		Hit hit = new Hit();
		hit.score = score;
		hit.doc = new Document();
		hit.doc.fields = Maps.newHashMap();
		
		for(int i = 0; i<fields.length; i+=2){
			hit.doc.fields.put(fields[i], fields[i+1]);
		}
		
		return hit;
	}
	
	protected IndexNodeDescriptor buildDescriptor(IndexNode.Iface client) throws IndexNodeDescriptorException{
		IndexNodeDescriptor descriptor = mock(IndexNodeDescriptor.class);
		when(descriptor.getClient()).thenReturn(client);
		
		return descriptor;
	}
	
	@Test
	public void testSearch() throws ParseException, NonExistentPartitionException, TException, SearcherException, IndexNodeDescriptorException {
		
		QueryResult qr1 = new QueryResult();
		qr1.parsedQuery = "test";
		qr1.totalHits = 123;
		qr1.hits = Lists.newArrayList(buildHit(0.1, "test", "field1"),
				buildHit(0.02, "test", "field2"),
				buildHit(0.03, "test", "field3"));
		
		QueryResult qr2 = new QueryResult();
		qr2.parsedQuery = "test";
		qr2.totalHits = 321;
		qr2.hits = Lists.newArrayList(buildHit(0.01, "test", "field4"),
				buildHit(0.2, "test", "field5"),
				buildHit(0.03, "test", "field6"));
		
		
		QueryResult qr3 = new QueryResult();
		qr3.parsedQuery = "test";
		qr3.totalHits = 456;
		qr3.hits = Lists.newArrayList(buildHit(0.01, "test", "field7"),
				buildHit(0.02, "test", "field8"),
				buildHit(0.3, "test", "field9"));
		
		IndexNode.Iface p1 = mock(IndexNode.Iface.class);
		IndexNode.Iface p2 = mock(IndexNode.Iface.class);
		IndexNode.Iface p3 = mock(IndexNode.Iface.class);
		
		when(p1.search(1, "test", 3, 0)).thenReturn(qr1);
		when(p2.search(2, "test", 3, 0)).thenReturn(qr2);
		when(p3.search(3, "test", 3, 0)).thenReturn(qr3);
		
		Multimap<Integer, IndexNodeDescriptor> partitions = LinkedListMultimap.create();
		partitions.put(1, buildDescriptor(p1));
		partitions.put(2, buildDescriptor(p2));
		partitions.put(3, buildDescriptor(p3));

		
		ParalellSearcher searcher = new ParalellSearcher();
		ParalellSearchResult result = searcher.parallelSearch(partitions, "test", 3, 0);
		
		assertEquals(900, result.qr.totalHits);
		assertEquals("test", result.qr.parsedQuery);
		assertEquals(0.3, result.qr.hits.get(0).score, 0.0001);
		assertEquals(0.2, result.qr.hits.get(1).score, 0.0001);
		assertEquals(0.1, result.qr.hits.get(2).score, 0.0001);
		
		assertEquals("field9", result.qr.hits.get(0).doc.fields.get("test"));
		assertEquals("field5", result.qr.hits.get(1).doc.fields.get("test"));
		assertEquals("field1", result.qr.hits.get(2).doc.fields.get("test"));


	}

}
