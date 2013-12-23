package ar.uba.fi.tppro.core.broker;

import java.util.List;
import java.util.Map;
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

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.StaleVersionException;
import ar.uba.fi.tppro.core.index.versionTracker.VersionTrackerServerException;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.IndexResult;
import ar.uba.fi.tppro.core.service.thrift.MessageId;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class ParalellIndexer {

	final Logger logger = LoggerFactory.getLogger(ParalellIndexer.class);

	private final NamedThreadFactory threadFactory = new NamedThreadFactory(
			"parallel-searcher");
	private final ExecutorService executorService = Executors
			.newCachedThreadPool(threadFactory);

	private LockManager lockManager;
	private GroupVersionTracker versionTracker;
	private VersionGenerator versionGenerator;

	protected long lockTimeout = 8000;
	protected long indexTimeout = 1000000;

	public ParalellIndexer(LockManager lockManager,
			GroupVersionTracker versionTracker, VersionGenerator versionGenerator) {
		this.lockManager = lockManager;
		this.versionTracker = versionTracker;
		this.versionGenerator = versionGenerator;
	}

	private class PartialList {
		int partitionId;
		IndexNodeDescriptor replica;
		List<Document> docs;

		@Override
		public String toString() {
			return String.format("PartitionId: %d, Replica:%s", partitionId, replica.toString());
		}
	}

	synchronized public IndexResult distributeAndIndex(final int shardId,
			Multimap<Integer, IndexNodeDescriptor> partitions,
			List<Document> documents) throws LockAquireTimeoutException,
			VersionTrackerServerException, StaleVersionException {

		IndexResult result = new IndexResult();
		result.errors = Lists.newArrayList();
		
		boolean hasFailures = false;

		Multimap<Integer, PartialList> partitionReplicas = LinkedListMultimap
				.create();

		for (Integer indexPartition : partitions.keySet()) {
			if (partitions.get(indexPartition).size() > 0) {
				for (IndexNodeDescriptor replica : partitions
						.get(indexPartition)) {
					PartialList replicaDocs = new PartialList();
					replicaDocs.replica = replica;
					replicaDocs.partitionId = indexPartition;
					replicaDocs.docs = Lists.newArrayList();

					partitionReplicas.put(indexPartition, replicaDocs);
				}
			} else {
				logger.warn("Partition " + indexPartition
						+ " does not have any assigned replica");
			}
		}

		Integer[] pIds = partitionReplicas.keySet().toArray(
				new Integer[partitionReplicas.keySet().size()]);

		int counter = 0;
		for (Document d : documents) {
			Integer pId = pIds[counter % pIds.length];

			for (PartialList replica : partitionReplicas.get(pId)) {
				replica.docs.add(d);
			}

			counter++;
		}

		logger.debug("Docs assigned to nodes: " + partitionReplicas);


		// Aquire lock
		IndexLock addLock = lockManager.aquire(shardId, 1000);
		try {
			// Get the current version
			final long previousVersion = versionTracker.getCurrentVersion(shardId);
			final long nextVersion = this.versionGenerator.getNextVersion();

			Map<Future<?>, PartialList> futures = Maps.newHashMap();

			for (final PartialList pl : partitionReplicas.values()) {

				Future<?> future = executorService.submit(new Runnable() {

					@Override
					public void run() {
						try {
							IndexNode.Iface client = pl.replica.getClient();
							client.prepareCommit(shardId, pl.partitionId,
									new MessageId(previousVersion, nextVersion), pl.docs);
						} catch (NonExistentPartitionException e) {
							throw new RuntimeException(e);
						} catch (TException e) {
							throw new RuntimeException(e);
						} catch (IndexNodeDescriptorException e1) {
							notifyNodeException(pl.replica, e1);
						}

					}
				});

				futures.put(future, pl);
			}

			for (Future<?> future : futures.keySet()) {
				try {
					future.get(indexTimeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					notifyError(futures.get(future), e);
					hasFailures = true;
					result.errors
							.add(new ar.uba.fi.tppro.core.service.thrift.Error(
									0, e.getMessage()));

				} catch (ExecutionException e) {
					Throwable wrapped = e.getCause();
					wrapped.printStackTrace();
					
					notifyError(futures.get(future), wrapped);
					hasFailures = true;
					result.errors
							.add(new ar.uba.fi.tppro.core.service.thrift.Error(
									0, wrapped.getMessage()));

				} catch (TimeoutException e) {
					notifyError(futures.get(future), e);
					hasFailures = true;
					result.errors
							.add(new ar.uba.fi.tppro.core.service.thrift.Error(
									0, e.getMessage()));
				}
			}

			//Ready to commit
			if (hasFailures == false) {
				versionTracker.setShardVersion(shardId, nextVersion);
			}

		} finally {
			addLock.release();
		}

		return result;
	}

	protected void notifyError(PartialList pl, Throwable e) {
		logger.error(String.format(
				"Indexing error: partitionId: %d, node: %s - %s",
				pl.partitionId, pl.replica, e));
	}

	private void notifyNodeException(IndexNodeDescriptor nodeDescriptor,
			IndexNodeDescriptorException e) {
		logger.error(String.format(
				"Exception in remote node %s: %s", nodeDescriptor, e.getMessage()));

	}

}
