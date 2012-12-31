package ar.uba.fi.tppro.core.index;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ParalellSearcher {
	
	final Logger logger = LoggerFactory.getLogger(ParalellSearcher.class);
	
	private final NamedThreadFactory threadFactory = new NamedThreadFactory("parallel-searcher");
	private final ExecutorService executorService = Executors.newCachedThreadPool(threadFactory);
	
	protected long timeout = 8000;
	
	
	public QueryResult parallelSearch(List<IndexPartitionClient> partitions, final String query, final int limit, final int offset){
		
		List<Future<QueryResult>> futures = Lists.newArrayList();
		
		for(final IndexPartitionClient p : partitions){
			
			Future<QueryResult> future = executorService.submit(new Callable<QueryResult>() {

				@Override
				public QueryResult call() throws Exception {
					return p.client.search(p.partitionId, query, limit, offset);
				}
			});
			
			futures.add(future);	
		}
		
		
		List<QueryResult> results = Lists.newArrayList();
		
        for (Future<QueryResult> f : futures)
        {
          try
          {
        	QueryResult res = f.get(timeout, TimeUnit.MILLISECONDS);
        	results.add(res);
          } catch(Exception e) {	          
            logger.error(e.getMessage(), e);
            //TODO: Procesar mejor los errores
          }
        }

        return ResultsMerger.mergeResults(results, limit);
				
	}

}
