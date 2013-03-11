package ar.uba.fi.tppro.core.index.versionTracker;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.shared.SharedCount;
import com.netflix.curator.framework.recipes.shared.SharedCountListener;
import com.netflix.curator.framework.recipes.shared.SharedCountReader;
import com.netflix.curator.framework.state.ConnectionState;

public class LocalShardVersionTracker implements ShardVersionTracker {
	
	final Logger logger = LoggerFactory.getLogger(LocalShardVersionTracker.class);

	private ConcurrentMap<Integer, AtomicInteger> versions = Maps.newConcurrentMap();
	private Multimap<Integer, ShardVersionObserver> observers = LinkedListMultimap.create();

	@Override
	public int getCurrentVersion(int shardId)
			throws VersionTrackerServerException {
		
		AtomicInteger version = versions.get(shardId);
		if(version == null){
			version = new AtomicInteger(0);
			versions.put(shardId, version);
		}
		
		return version.get();
	
	}

	@Override
	public void setShardVersion(int shardId, int newVersion)
			throws StaleVersionException, VersionTrackerServerException {
		
		AtomicInteger version = versions.get(shardId);
		if(version == null){
			version = new AtomicInteger(0);
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
