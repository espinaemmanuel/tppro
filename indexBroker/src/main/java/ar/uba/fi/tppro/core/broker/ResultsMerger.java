package ar.uba.fi.tppro.core.broker;

import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.PriorityQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ResultsMerger {
	
	private class HitsPriorityQueue extends PriorityQueue<Hit>{

		public HitsPriorityQueue(int maxSize) {
			super(maxSize);
		}

		@Override
		protected boolean lessThan(Hit a, Hit b) {
			return Double.compare(a.score, b.score) < 0;
		}
		
	}
	
	public QueryResult mergeResults(List<QueryResult> results, int numResults) throws ResultsMergerException{
		HitsPriorityQueue priorityQueue = new HitsPriorityQueue(numResults);
		
		int totalHits = 0;
		String parsedQuery = results.get(0).parsedQuery;
		
		for(QueryResult r : results){
			for(Hit h : r.hits){
				priorityQueue.insertWithOverflow(h);
			}
			totalHits += r.totalHits;
			
			if(!r.parsedQuery.equals(parsedQuery)){
				throw new ResultsMergerException("Incompatible result. Expected '" + parsedQuery + "' but was '" + r.parsedQuery);
			}
		}
		
		int resultsSize = Math.min(numResults, priorityQueue.size());
		Hit[] hits = new Hit[resultsSize];
		for(int i= resultsSize-1; i >= 0; i--){
			hits[i] = priorityQueue.pop();
		}
		
		QueryResult mergedResults = new QueryResult();
		mergedResults.totalHits = totalHits;
		mergedResults.parsedQuery = results.get(0).parsedQuery;
		mergedResults.hits = Lists.newArrayList(hits);
		
		return mergedResults;
	}

}
