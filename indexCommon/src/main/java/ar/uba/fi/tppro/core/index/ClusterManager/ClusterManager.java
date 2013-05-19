package ar.uba.fi.tppro.core.index.ClusterManager;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

public interface ClusterManager {
	
	public enum NodeType {
		INDEX, BROKER
	}

	void registerNode(IndexNodeDescriptor localNodeDescriptor,
			NodeType index) throws Exception;

}
