package ar.uba.fi.tppro.core.index;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.lucene.util.NamedThreadFactory;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class ParalellIndexer {

	final Logger logger = LoggerFactory.getLogger(ParalellIndexer.class);

	private final NamedThreadFactory threadFactory = new NamedThreadFactory(
			"parallel-searcher");
	private final ExecutorService executorService = Executors
			.newCachedThreadPool(threadFactory);

	protected long timeout = 8000;

	private class PartialList {
		int partitionId;
		IndexNodeDescriptor replica;
		List<Document> docs;
	}

	public void distributeAndIndex(
			Multimap<Integer, IndexNodeDescriptor> partitions,
			List<Document> documents) {

		Multimap<Integer, PartialList> partialLists = LinkedListMultimap
				.create();

		for (Integer indexPartition : partitions.keys()) {
			if (partitions.get(indexPartition).size() > 0) {
				for (IndexNodeDescriptor replica : partitions
						.get(indexPartition)) {
					PartialList partialList = new PartialList();
					partialList.replica = replica;
					partialList.partitionId = indexPartition;
					partialList.docs = Lists.newArrayList();

					partialLists.put(indexPartition, partialList);
				}
			} else {
				logger.warn("Partition " + indexPartition
						+ " does not have any assigned replica");
			}
		}

		Integer[] pIds = partialLists.keySet().toArray(
				new Integer[partialLists.keySet().size()]);

		int counter = 0;
		for (Document d : documents) {
			Integer pId = pIds[counter % pIds.length];

			for (PartialList partialList : partialLists.get(pId)) {
				partialList.docs.add(d);
			}

			counter++;
		}

		List<Future<?>> futures = Lists.newArrayList();

		for (final PartialList pl : partialLists.values()) {

			Future<?> future = executorService.submit(new Runnable() {

				@Override
				public void run() {
					IndexNode.Client client = pl.replica.getClient();
					try {
						client.index(pl.partitionId, pl.docs);
					} catch (NonExistentPartitionException e) {
						throw new RuntimeException("The partition "
								+ pl.partitionId + " does not exist in node "
								+ pl.replica, e);
					} catch (TException e) {
						throw new RuntimeException(
								"Could not index in partition "
										+ pl.partitionId + " in the node "
										+ pl.replica, e);
					}
				}
			});

			futures.add(future);
		}
		
		for(Future<?> future : futures){
			try {
				future.get();
			} catch (InterruptedException e) {
				logger.error("Thread interrputed", e);
			} catch (ExecutionException e) {
				logger.error("Execution exception", e);
			}
		}
	}

}
