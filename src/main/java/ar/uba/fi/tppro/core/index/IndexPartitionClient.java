package ar.uba.fi.tppro.core.index;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class IndexPartitionClient{
	IndexNode.Client client;
	int partitionId;
}
