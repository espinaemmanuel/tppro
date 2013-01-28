package ar.uba.fi.tppro.core.broker;

import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class IndexPartitionClient{
	IndexNode.Client client;
	int partitionId;
}
