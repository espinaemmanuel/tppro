package ar.uba.fi.tppro.tests;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellIndexException;
import ar.uba.fi.tppro.tests.ReutersSaxParser.ReutersDoc;
import ar.uba.fi.tppro.tests.ReutersSaxParser.ReutersDocHandler;

public class IndexingReutersDocHandler implements ReutersDocHandler{
	
	private IndexBroker.Iface broker;
	private int bufferSize;
	private int shard;
	
	private long indexTime = 0;
	
	public long getIndexTime() {
		return indexTime;
	}

	List<Document> documentsBuffer = Lists.newArrayList();
	AtomicInteger counter;
	
	public IndexingReutersDocHandler(IndexBroker.Iface broker, int bufferSize, int shard, AtomicInteger counter){
		this.broker = broker;
		this.bufferSize = bufferSize;
		this.shard = shard;
		this.counter = counter;
	}

	@Override
	public void handle(ReutersDoc doc) {
		
		Document searchDoc = new Document();
		searchDoc.fields = Maps.newHashMap();
		
		searchDoc.fields.put("title", doc.title);
		searchDoc.fields.put("text", doc.text);
		
		documentsBuffer.add(searchDoc);
		
		if(documentsBuffer.size() >= bufferSize){
			try {
				
				long before = System.nanoTime();
				broker.index(this.shard, documentsBuffer);
				long after = System.nanoTime();
				
				counter.addAndGet(documentsBuffer.size());
				
				indexTime += after - before;
				
			} catch (TException e) {
				e.printStackTrace();
			}
			documentsBuffer.clear();
		}		
	}

}
