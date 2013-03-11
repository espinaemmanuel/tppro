package ar.uba.fi.tppro.core.index.versionTracker;

public interface ShardVersionTracker {

	public int getCurrentVersion(int shardId) throws VersionTrackerServerException;

	public void setShardVersion(int shardId, int newVersion) throws StaleVersionException, VersionTrackerServerException;

	public void addVersionObserver(int shardId, ShardVersionObserver observer);

}
