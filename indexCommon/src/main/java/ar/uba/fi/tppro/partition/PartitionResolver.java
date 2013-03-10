package ar.uba.fi.tppro.partition;

import java.util.List;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;

import com.google.common.collect.Multimap;

public interface PartitionResolver {

	public Multimap<Integer, IndexNodeDescriptor> resolve(
			List<Integer> partitionIds) throws PartitionResolverException;

	/**
	 * Registers or updates a partition
	 * 
	 * @param partitionId
	 * @param descriptor
	 * @param status
	 * @throws PartitionResolverException
	 */
	void registerPartition(int partitionId, IndexNodeDescriptor descriptor,
			IndexPartitionStatus status) throws PartitionResolverException;

}
