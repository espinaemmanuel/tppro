package ar.uba.fi.tppro.core.index.versionTracker;

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.index.IndexPartition;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.shared.SharedCount;
import com.netflix.curator.framework.recipes.shared.SharedCountListener;
import com.netflix.curator.framework.recipes.shared.SharedCountReader;
import com.netflix.curator.framework.state.ConnectionState;

public class PartitionVersionTracker {
	
	final Logger logger = LoggerFactory.getLogger(PartitionVersionTracker.class);

	private static final String PARTITION_VERSIONS = "/partitionVersions/";
	private CuratorFramework client;
	private ConcurrentMap<Integer, SharedCount> versionCounters = Maps
			.newConcurrentMap();
	
	private Multimap<Integer, PartitionVersionObserver> partitionObservers = HashMultimap.create();

	public PartitionVersionTracker(CuratorFramework client) {
		this.client = client;
	}
	
	protected void notifyStateChange(int partitionId, ConnectionState state){
		switch(state){
		case CONNECTED:
			logger.info("Partition " + partitionId + "connected to partition tracker");
			break;
			
		case RECONNECTED:
			logger.info("Partition " + partitionId + "conection to partition tracker restored");
			break;
			
		case SUSPENDED:
			logger.info("Partition " + partitionId + "conection to partition tracker momentarily suspended");
			break;
			
		case LOST:
			logger.info("Partition " + partitionId + "conection to partition tracker lost");
			for(PartitionVersionObserver observer : partitionObservers.get(partitionId)){
				observer.onConectionLoss(partitionId);
			}
			break;			
		}
		
	}

	protected SharedCount initializeCounter(final int partitionId) throws Exception {
		SharedCount newCounter = new SharedCount(this.client,
				PARTITION_VERSIONS + partitionId, 0);
		
		newCounter.addListener(new SharedCountListener() {

			@Override
			public void stateChanged(CuratorFramework client,
					ConnectionState newState) {
				notifyStateChange(partitionId, newState);
			}

			@Override
			public void countHasChanged(SharedCountReader sharedCount,
					int newCount) throws Exception {
				
				for(PartitionVersionObserver observer : partitionObservers.get(partitionId)){
					observer.onVersionChanged(partitionId, newCount);
				}

			}
		});

		newCounter.start();

		return newCounter;
	}

	public int getCurrentVersion(int partitionId)
			throws VersionTrackerServerException {
		try {

			SharedCount counter = versionCounters.get(partitionId);

			if (counter == null) {
				counter = this.initializeCounter(partitionId);
				versionCounters.put(partitionId, counter);
			}

			return counter.getCount();

		} catch (Exception e) {
			throw new VersionTrackerServerException(
					"could not initialize the counter for partition "
							+ partitionId, e);
		}
	}

	public void setPartitionVersion(int partitionId, int newVersion)
			throws StaleVersionException, VersionTrackerServerException {
		SharedCount counter = versionCounters.get(partitionId);

		try {
			if (counter == null) {
				counter = this.initializeCounter(partitionId);
				versionCounters.put(partitionId, counter);
			}
		} catch (Exception e) {
			throw new VersionTrackerServerException(
					"could not initialize the counter for partition "
							+ partitionId, e);
		}

		try {
			boolean changedVersion = counter.trySetCount(newVersion);
			if (changedVersion == false) {
				throw new StaleVersionException(counter.getCount());
			}
		} catch (Exception e) {
			throw new VersionTrackerServerException(
					"Low level exception in the server", e);
		}
	}

	public void addVersionObserver(int partitionId,
			PartitionVersionObserver observer)
			throws VersionTrackerServerException {
	}

}
