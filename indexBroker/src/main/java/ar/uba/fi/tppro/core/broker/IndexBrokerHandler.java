package ar.uba.fi.tppro.core.broker;

import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import ar.uba.fi.tppro.core.index.BrokerIterface;
import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexResult;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellIndexException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class IndexBrokerHandler implements BrokerIterface {

	final Logger logger = LoggerFactory.getLogger(IndexBrokerHandler.class);
	
	private ParalellSearcher searcher;
	private ParalellIndexer indexer;
	private LockManager lockManager;
	private PartitionResolver resolver;
	

	public IndexBrokerHandler(PartitionResolver resolver, LockManager lockManager) {
		super();
		this.resolver = resolver;
		this.lockManager = lockManager;
		this.searcher = new ParalellSearcher();
		this.indexer = new ParalellIndexer(this.lockManager);
	}

	@Override
	public ParalellSearchResult search(List<Integer> partitionIds, String query,
			int limit, int offset) throws ParalellSearchException, NonExistentPartitionException, TException {
		
		Multimap<Integer, IndexNodeDescriptor> partitionsMap = resolver
				.resolve(partitionIds);

		try {
			// Check existence of partitions
			List<Integer> emptyPartitions = Lists.newArrayList();
			for (Integer pId : partitionsMap.keySet()) {
				if (partitionsMap.get(pId).size() == 0) {
					emptyPartitions.add(pId);
				}
			}
			if (emptyPartitions.size() > 0) {
				throw new NonExistentPartitionException(emptyPartitions);
			}

			try {
				return searcher.parallelSearch(partitionsMap, query, limit,
						offset);
			} catch (SearcherException e) {
				throw new ParalellSearchException("SearcherException: "
						+ e.getMessage());
			}
		} finally {
			for (IndexNodeDescriptor descriptor : partitionsMap.values()) {
				descriptor.close();
			}
		}
	}

	@Override
	public IndexResult deleteByQuery(List<Integer> partitionId, String query)
			throws TException {
		return null;		
	}

	@Override
	public IndexResult index(List<Integer> partitionIds,
			List<Document> documents) throws ParalellIndexException,
			NonExistentPartitionException, TException {

		Multimap<Integer, IndexNodeDescriptor> partitionsMap = resolver
				.resolve(partitionIds);

		try {
			// Check existence of partitions
			List<Integer> emptyPartitions = Lists.newArrayList();
			for (Integer pId : partitionsMap.keySet()) {
				if (partitionsMap.get(pId).size() == 0) {
					emptyPartitions.add(pId);
				}
			}
			if (emptyPartitions.size() > 0) {
				throw new NonExistentPartitionException(emptyPartitions);
			}

			try {
				return this.indexer
						.distributeAndIndex(partitionsMap, documents);
			} catch (LockAquireTimeoutException e) {
				throw new ParalellIndexException("LockAquireTimeoutException: "
						+ e.getMessage());
			}
		} finally {
			for (IndexNodeDescriptor descriptor : partitionsMap.values()) {
				descriptor.close();
			}
		}
	}
}
