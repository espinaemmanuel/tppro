package ar.uba.fi.tppro.core.index.versionTracker;

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.shared.SharedCount;
import com.netflix.curator.framework.recipes.shared.SharedCountListener;
import com.netflix.curator.framework.recipes.shared.SharedCountReader;
import com.netflix.curator.framework.state.ConnectionState;

public class ZkShardVersionTracker implements ShardVersionTracker {
	
	final Logger logger = LoggerFactory.getLogger(ZkShardVersionTracker.class);

	private static final String SHARD_VERSIONS = "/shardVersions/";
	private CuratorFramework client;
	private ConcurrentMap<Integer, SharedCount> versionCounters = Maps
			.newConcurrentMap();
	
	private Multimap<Integer, ShardVersionObserver> shardObservers = HashMultimap.create();

	public ZkShardVersionTracker(CuratorFramework client) {
		this.client = client;
	}
	
	protected void notifyStateChange(int shardId, ConnectionState state){
		switch(state){
		case CONNECTED:
			logger.info("Shard " + shardId + "connected to partition tracker");
			break;
			
		case RECONNECTED:
			logger.info("Shard " + shardId + "conection to partition tracker restored");
			break;
			
		case SUSPENDED:
			logger.info("Shard " + shardId + "conection to partition tracker momentarily suspended");
			break;
			
		case LOST:
			logger.info("Shard " + shardId + "conection to partition tracker lost");
			for(ShardVersionObserver observer : shardObservers.get(shardId)){
				observer.onConectionLoss(shardId);
			}
			break;			
		}
		
	}

	protected SharedCount initializeCounter(final int shardId) throws Exception {
		SharedCount newCounter = new SharedCount(this.client,
				SHARD_VERSIONS + shardId, 0);
		
		newCounter.addListener(new SharedCountListener() {

			@Override
			public void stateChanged(CuratorFramework client,
					ConnectionState newState) {
				notifyStateChange(shardId, newState);
			}

			@Override
			public void countHasChanged(SharedCountReader sharedCount,
					int newCount) throws Exception {
				
				for(ShardVersionObserver observer : shardObservers.get(shardId)){
					observer.onVersionChanged(shardId, newCount);
				}

			}
		});

		newCounter.start();

		return newCounter;
	}

	@Override
	public int getCurrentVersion(int shardId)
			throws VersionTrackerServerException {
		try {

			SharedCount counter = versionCounters.get(shardId);

			if (counter == null) {
				counter = this.initializeCounter(shardId);
				versionCounters.put(shardId, counter);
			}

			return counter.getCount();

		} catch (Exception e) {
			throw new VersionTrackerServerException(
					"could not initialize the counter for partition "
							+ shardId, e);
		}
	}

	@Override
	public void setShardVersion(int shardId, int newVersion)
			throws StaleVersionException, VersionTrackerServerException {
		SharedCount counter = versionCounters.get(shardId);

		try {
			if (counter == null) {
				counter = this.initializeCounter(shardId);
				versionCounters.put(shardId, counter);
			}
		} catch (Exception e) {
			throw new VersionTrackerServerException(
					"could not initialize the counter for partition "
							+ shardId, e);
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

	@Override
	public void addVersionObserver(int shardId,
			ShardVersionObserver observer) {
		this.shardObservers.put(shardId, observer);
	}

}
