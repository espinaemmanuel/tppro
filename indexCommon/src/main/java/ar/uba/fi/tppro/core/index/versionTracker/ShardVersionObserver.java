package ar.uba.fi.tppro.core.index.versionTracker;

public interface ShardVersionObserver {

	void onConectionLoss(int shardId);

	void onVersionChanged(int shardId, long newVersion);

}
