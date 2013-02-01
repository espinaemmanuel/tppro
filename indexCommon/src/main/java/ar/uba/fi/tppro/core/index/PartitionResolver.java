package ar.uba.fi.tppro.core.index;

import java.util.List;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

import com.google.common.collect.Multimap;

public interface PartitionResolver {

	public Multimap<Integer, IndexNodeDescriptor> resolve(
			List<Integer> partitionIds);

}
