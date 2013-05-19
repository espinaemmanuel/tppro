package ar.uba.fi.tppro.core.index.versionTracker;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.shared.SharedCount;
import com.netflix.curator.framework.recipes.shared.SharedCountListener;
import com.netflix.curator.framework.recipes.shared.SharedCountReader;
import com.netflix.curator.framework.recipes.shared.SharedValue;
import com.netflix.curator.framework.recipes.shared.SharedValueListener;
import com.netflix.curator.framework.recipes.shared.SharedValueReader;
import com.netflix.curator.framework.state.ConnectionState;

public class ZkShardVersionTracker implements GroupVersionTracker {
	
	final Logger logger = LoggerFactory.getLogger(ZkShardVersionTracker.class);

	private static final String SHARD_VERSIONS = "/shardVersions";
	private CuratorFramework client;
	
	private ConcurrentMap<Integer, SharedValue> versionCounters = Maps
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

	protected SharedValue initializeCounter(final int shardId) throws Exception {
		SharedValue newCounter = new SharedValue(this.client,
				SHARD_VERSIONS + "/" + shardId, toBytes(0l));
		
		SharedValueListener valueListener = new SharedValueListener()
        {
            @Override
            public void valueHasChanged(SharedValueReader sharedValue, byte[] newValue) throws Exception
            {
				for(ShardVersionObserver observer : shardObservers.get(shardId)){
					observer.onVersionChanged(shardId, fromBytes(newValue));
				}
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
            	notifyStateChange(shardId, newState);
            }
        };
        
        newCounter.getListenable().addListener(valueListener, MoreExecutors.sameThreadExecutor());
		newCounter.start();

		return newCounter;
	}

	@Override
	public long getCurrentVersion(int shardId)
			throws VersionTrackerServerException {
		try {

			SharedValue counter = versionCounters.get(shardId);

			if (counter == null) {
				counter = this.initializeCounter(shardId);
				versionCounters.put(shardId, counter);
			}

			return fromBytes(counter.getValue());

		} catch (Exception e) {
			throw new VersionTrackerServerException(
					"could not initialize the counter for partition "
							+ shardId, e);
		}
	}
	
	@Override
	public Map<Integer, Long> getAllVersions() throws VersionTrackerServerException{
		
		List<String> groups = Lists.newArrayList();
		Map<Integer, Long> versionMap = Maps.newHashMap();
		
		try {
			groups.addAll(this.client.getChildren().forPath(SHARD_VERSIONS));
		} catch (Exception e) {
			logger.info("no group found");
		}
		
		for(String groupStr : groups){
			Integer groupId = Integer.parseInt(groupStr);
			long version = this.getCurrentVersion(groupId);
			
			versionMap.put(groupId, version);
		}
		
		return versionMap;

	}

	@Override
	public void setShardVersion(int shardId, long newVersion)
			throws StaleVersionException, VersionTrackerServerException {
		SharedValue counter = versionCounters.get(shardId);

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
			boolean changedVersion = counter.trySetValue(toBytes(newVersion));
			if (changedVersion == false) {
				throw new StaleVersionException(fromBytes(counter.getValue()));
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
	
    private byte[] toBytes(long value)
    {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putLong(value);
        return bytes;
    }
    
    private long fromBytes(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes).getLong();
    }

}
