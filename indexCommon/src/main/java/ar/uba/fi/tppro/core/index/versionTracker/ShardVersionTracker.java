package ar.uba.fi.tppro.core.index.versionTracker;

import java.util.Map;

public interface ShardVersionTracker {

	public long getCurrentVersion(int shardId) throws VersionTrackerServerException;

	public void setShardVersion(int shardId, long newVersion) throws StaleVersionException, VersionTrackerServerException;

	public void addVersionObserver(int shardId, ShardVersionObserver observer);

	Map<Integer, Long> getAllVersions() throws VersionTrackerServerException;

}
