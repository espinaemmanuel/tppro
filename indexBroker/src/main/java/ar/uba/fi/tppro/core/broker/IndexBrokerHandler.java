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
import ar.uba.fi.tppro.core.index.versionTracker.ShardVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.StaleVersionException;
import ar.uba.fi.tppro.core.index.versionTracker.VersionTrackerServerException;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexResult;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellIndexException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;
import ar.uba.fi.tppro.partition.PartitionResolver;
import ar.uba.fi.tppro.partition.PartitionResolverException;

public class IndexBrokerHandler implements BrokerIterface {

	final Logger logger = LoggerFactory.getLogger(IndexBrokerHandler.class);

	private ParalellSearcher searcher;
	private ParalellIndexer indexer;
	private LockManager lockManager;
	private ShardVersionTracker versionTracker;

	private PartitionResolver resolver;

	public IndexBrokerHandler(PartitionResolver resolver,
			LockManager lockManager, ShardVersionTracker versionTracker, VersionGenerator versionGenerator) {
		super();
		this.resolver = resolver;
		this.lockManager = lockManager;
		this.versionTracker = versionTracker;
		this.searcher = new ParalellSearcher();
		this.indexer = new ParalellIndexer(this.lockManager,
				this.versionTracker, versionGenerator);
	}

	@Override
	public ParalellSearchResult search(int shardId, String query, int limit,
			int offset) throws ParalellSearchException,
			NonExistentPartitionException, TException {

		Multimap<Integer, IndexNodeDescriptor> partitionsMap;
		try {
			partitionsMap = resolver.resolve(shardId);
		} catch (PartitionResolverException e) {
			logger.error("Could not resolve partitions", e);
			throw new ParalellSearchException("Could not resolve partitions");
		}

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
				return searcher.parallelSearch(shardId, partitionsMap, query, limit,
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
	public IndexResult deleteByQuery(int shardId, String query)
			throws TException {
		return null;
	}

	@Override
	public IndexResult index(int shardId,
			List<Document> documents) throws ParalellIndexException,
			NonExistentPartitionException, TException {

		Multimap<Integer, IndexNodeDescriptor> partitionsMap;
		try {
			partitionsMap = resolver.resolve(shardId);
		} catch (PartitionResolverException e) {
			logger.error("Could not resolve partitions", e);
			throw new ParalellSearchException("Could not resolve partitions");
		}

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
						.distributeAndIndex(shardId, partitionsMap, documents);
			} catch (LockAquireTimeoutException e) {
				throw new ParalellIndexException("LockAquireTimeoutException: "
						+ e.getMessage());
			} catch (VersionTrackerServerException e) {
				throw new ParalellIndexException("VersionTrackerServerException: "
						+ e.getMessage());
			} catch (StaleVersionException e) {
				throw new ParalellIndexException("StaleVersionException: "
						+ e.getMessage());
			}
		} finally {
			for (IndexNodeDescriptor descriptor : partitionsMap.values()) {
				descriptor.close();
			}
		}
	}
}
