package ar.uba.fi.tppro.core.index.versionTracker;

public interface PartitionVersionObserver {

	void onConectionLoss(int partitionId);

	void onVersionChanged(int partitionId, int newVersion);

}
