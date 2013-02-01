package ar.uba.fi.tppro.core.index;

public interface PartitionStatusObserver {

	void notifyStatusChange(IndexPartition indexPartition);

}
