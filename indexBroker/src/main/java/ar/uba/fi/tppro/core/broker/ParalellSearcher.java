package ar.uba.fi.tppro.core.broker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.util.NamedThreadFactory;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ParalellSearcher {
	
	final Logger logger = LoggerFactory.getLogger(ParalellSearcher.class);
	
	private final NamedThreadFactory threadFactory = new NamedThreadFactory("parallel-searcher");
	private final ExecutorService executorService = Executors.newCachedThreadPool(threadFactory);
	
	protected long timeout = 800000;
	
	public ParalellSearchResult parallelSearch(final int shardId, final Multimap<Integer, IndexNodeDescriptor> partitions, final String query, final int limit, final int offset) throws SearcherException{
		
		Map<Future<QueryResult>, Integer> futuresMap = Maps.newHashMap();
		
		for(final Integer pId : partitions.keySet()){
			Future<QueryResult> future = executorService.submit(new Callable<QueryResult>() {

				@Override
				public QueryResult call() throws Exception {
					
					List<IndexNodeDescriptor> partitionList = Lists.newArrayList(partitions.get(pId));
					Collections.shuffle(partitionList);
									
					Map<IndexNodeDescriptor, Exception> coughtExceptions = Maps.newHashMap();
					
					for(int i=0; i < partitionList.size() ; i++){
						IndexNodeDescriptor indexNode = partitionList.get(i);
												
						try {
							
							QueryResult result = indexNode.getClient().search(shardId, pId, query, limit, offset);
							
							if(result != null){							
								processExceptions(pId, coughtExceptions);
								return result;
							}	
						} catch (IndexNodeDescriptorException e){
							logger.info("CLUSTER FAILURE: Could not connect to node " + indexNode + ". A replica will be used", e);
							coughtExceptions.put(indexNode, e);
						} catch (ParseException e) {
							coughtExceptions.put(indexNode, e);
						} catch (NonExistentPartitionException e) {
							coughtExceptions.put(indexNode, e);
						} catch (TException e) {
							coughtExceptions.put(indexNode, e);
						}	
					}
					
					throw new BrokenPartitionException("Could not resolve the search on any replica", coughtExceptions);
				}
			});
			
			futuresMap.put(future, pId);	
		}
		
		List<QueryResult> results = Lists.newArrayList();
		
        for (Future<QueryResult> f : futuresMap.keySet())
        {
			try {				
				results.add(f.get(timeout, TimeUnit.MILLISECONDS));
			} catch (InterruptedException e) {
				throw new SearcherException("Partition searcher thread interrputed. PartitionID: " + futuresMap.get(f), e);
			} catch (ExecutionException e) {
				throw new SearcherException("Could not search on partition " + futuresMap.get(f), e);
			} catch (TimeoutException e) {
				throw new SearcherException(String.format("Search on one partition %d took too long (%d ms), aborting", futuresMap.get(f), timeout) , e);
			}
        }
        
        ResultsMerger merger = new ResultsMerger();

        try {
        	QueryResult qr = merger.mergeResults(results, limit);
        	ParalellSearchResult result = new ParalellSearchResult();
        	result.qr = qr;
        	result.errors = Lists.newArrayList();
        	
        	return result;
		} catch (ResultsMergerException e) {
			throw new SearcherException("Cannot merge the partial results", e);
		}		
	}
	
	protected void processExceptions(int partitionId, Map<IndexNodeDescriptor, Exception> coughtExceptions){
		//TODO: process errors
	}

}
