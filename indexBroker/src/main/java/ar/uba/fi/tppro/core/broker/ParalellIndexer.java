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
import ar.uba.fi.tppro.core.index.lock.LockManager.LockType;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.IndexResult;
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

	protected long lockTimeout = 8000;
	protected long indexTimeout = 1000000;
	
	public ParalellIndexer(LockManager lockManager){
		this.lockManager = lockManager;
	}

	private class PartialList {
		int partitionId;
		IndexNodeDescriptor replica;
		List<Document> docs;
		
		@Override
		public String toString(){
			return String.format("Replica:%s , Docs:%s", replica.toString(), docs.toString());
		}
	}

	public IndexResult distributeAndIndex(Multimap<Integer, IndexNodeDescriptor> partitions, List<Document> documents)
			throws LockAquireTimeoutException {

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
		
		IndexLock addLock = lockManager.aquire(LockType.ADD, 1000);
		
		Map<Future<?>, PartialList> futures = Maps.newHashMap();

		for (final PartialList pl : partitionReplicas.values()) {

			Future<?> future = executorService.submit(new Runnable() {

				@Override
				public void run() {
					try {
						IndexNode.Iface client = pl.replica.getClient();
						client.index(pl.partitionId, pl.docs);
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
		
		for(Future<?> future : futures.keySet()){
			try {
				future.get(indexTimeout, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				notifyError(futures.get(future), e.getCause().getCause());
			} catch (ExecutionException e) {
				notifyError(futures.get(future), e.getCause().getCause());
			} catch (TimeoutException e) {
				notifyError(futures.get(future), e.getCause().getCause());
			}
		}
		
		addLock.release();
		
		IndexResult result = new IndexResult();
		return result;
	}
	
	protected void notifyError(PartialList pl, Throwable e){
		logger.error(String.format("Indexing error: partitionId: %d, node: %s - %s", pl.partitionId, pl.replica, e));
		//TODO
	}
	
	private void notifyNodeException(IndexNodeDescriptor replica,
			IndexNodeDescriptorException e) {
		logger.error(String.format("Exception getting client for node: %d, node: %s - %s", replica, e.getMessage()));
		
	}

}
