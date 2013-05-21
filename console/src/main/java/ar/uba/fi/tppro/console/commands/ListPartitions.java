package ar.uba.fi.tppro.console.commands;

import java.util.List;


import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.PartitionStatus;

public class ListPartitions implements Command {

	public String getName() {
		return "partitions";
	}

	public void execute(String[] argv, Context context) throws Exception {
		if (context.currentNode == null) {
			System.out.println("Current node not selected");
		}

		String[] nodeParts = context.currentNode.split("_");

		RemoteIndexNodeDescriptor remoteNode = new RemoteIndexNodeDescriptor(
				nodeParts[0], Integer.parseInt(nodeParts[1]));
		IndexNode.Iface indexNode = remoteNode.getClient();

		List<PartitionStatus> partitions = indexNode.listPartitions();

		if (partitions.size() == 0) {
			System.out.println("The node has no partitions");
		} else {
			for (PartitionStatus partition : partitions) {
				System.out.println(String.format(
						"Group: %d, Partition: %d, Status: %s",
						partition.groupId, partition.partitionId,
						partition.status));
			}
		}
	}
}
