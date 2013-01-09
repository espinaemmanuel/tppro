package ar.uba.fi.tppro.core.index;

import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ResultsMerger {
	
	public static QueryResult mergeResults(List<QueryResult> results, int numResults){
		MinMaxPriorityQueue<Hit> priorityQueue = MinMaxPriorityQueue.orderedBy(new Comparator<Hit>() {

			@Override
			public int compare(Hit o1, Hit o2) {
				
				if(o1.score > o2.score){
					return 1;
				} else if (o1.score < o2.score){
					return -1;
				} else {
					return 0;
				}
			}
		}).maximumSize(numResults).create();
		
		int totalHits = 0;
		for(QueryResult r : results){
			priorityQueue.addAll(r.hits);
			totalHits += r.totalHits;
		}
		
		QueryResult mergedResults = new QueryResult();
		mergedResults.totalHits = totalHits;
		mergedResults.hits = Lists.newArrayList(priorityQueue);
		
		return mergedResults;
	}

}
