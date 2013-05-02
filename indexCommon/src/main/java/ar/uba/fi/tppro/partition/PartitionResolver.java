package ar.uba.fi.tppro.partition;

import java.util.List;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;

import com.google.common.collect.Multimap;

public interface PartitionResolver {

	public Multimap<Integer, IndexNodeDescriptor> resolve(int shardId) throws PartitionResolverException;

	/**
	 * Registers or updates a partition
	 * 
	 * @param partitionId
	 * @param descriptor
	 * @param status
	 * @throws PartitionResolverException
	 */
	void registerPartition(int shardId, int partitionId, IndexNodeDescriptor descriptor,
			IndexPartitionStatus status) throws PartitionResolverException;
	
	public void updatePartitionStatus(int shardId, int partitionId,
			IndexNodeDescriptor descriptor, IndexPartitionStatus status) throws PartitionResolverException;

	public List<PartitionDescriptor> getAll() throws PartitionResolverException;

}
