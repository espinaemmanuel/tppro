package ar.uba.fi.tppro.core.index.versionTracker;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class LocalShardVersionTracker implements ShardVersionTracker {
	
	final Logger logger = LoggerFactory.getLogger(LocalShardVersionTracker.class);

	private ConcurrentMap<Integer, AtomicLong> versions = Maps.newConcurrentMap();
	private Multimap<Integer, ShardVersionObserver> observers = LinkedListMultimap.create();

	@Override
	public long getCurrentVersion(int shardId)
			throws VersionTrackerServerException {
		
		AtomicLong version = versions.get(shardId);
		if(version == null){
			version = new AtomicLong(0);
			versions.put(shardId, version);
		}
		
		return version.get();
	
	}

	@Override
	public void setShardVersion(int shardId, long newVersion)
			throws StaleVersionException, VersionTrackerServerException {
		
		AtomicLong version = versions.get(shardId);
		if(version == null){
			version = new AtomicLong(0);
			versions.put(shardId, version);
		}
		
		version.set(newVersion);
		
		if(observers.get(shardId).size() > 0){
			for(ShardVersionObserver observer : observers.get(shardId)){
				observer.onVersionChanged(shardId, newVersion);
			}
		}
	}

	@Override
	public void addVersionObserver(int shardId,
			ShardVersionObserver observer) {
		this.observers.put(shardId, observer);
	}

}
