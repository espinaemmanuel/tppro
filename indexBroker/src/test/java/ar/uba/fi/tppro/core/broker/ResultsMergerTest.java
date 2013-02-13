package ar.uba.fi.tppro.core.broker;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class ResultsMergerTest {
	
	protected List<QueryResult> generateResutls(){
		List<QueryResult> results = Lists.newArrayList();
		
		for(int i=0; i<10; i++){
			QueryResult qr = new QueryResult();
			qr.hits = Lists.newArrayList();
			qr.parsedQuery = "test";
			qr.totalHits = 10*(i+1);
			results.add(qr);
			
			for(int j=0; j<10; j++){
				Hit hit = new Hit();
				hit.score = Math.random();
				
				qr.hits.add(hit);
			}
		}
				
		return results;
	}

	@Test
	public void testMerge() throws ResultsMergerException {
		List<QueryResult> generatedResults = generateResutls();
		
		List<Double> scores = Lists.newArrayList();
		
		for(QueryResult qr : generatedResults){
			for(Hit hit : qr.hits){
				scores.add(hit.score);
			}
		}
		
		Collections.sort(scores, Collections.reverseOrder());
		
		ResultsMerger merger =  new ResultsMerger();
		
		QueryResult mergedResults = merger.mergeResults(generatedResults, 10);
		
		assertEquals(550, mergedResults.totalHits);
		
		for(int i = 0; i < 10; i++){
			assertEquals(scores.get(i), new Double(mergedResults.hits.get(i).score));
		}
		
	}

}
