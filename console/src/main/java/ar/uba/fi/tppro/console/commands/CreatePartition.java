package ar.uba.fi.tppro.console.commands;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;

public class CreatePartition implements Command {

	public String getName() {
		return "createPartition";
	}

	public void execute(String[] argv, Context context) throws Exception {
		int groupId = Integer.parseInt(argv[1]);
		int partitionId = Integer.parseInt(argv[2]);
		
		if (context.currentNode == null) {
			System.out.println("Current node not selected");
		}

		String[] nodeParts = context.currentNode.split("_");

		RemoteIndexNodeDescriptor remoteNode = new RemoteIndexNodeDescriptor(
				nodeParts[0], Integer.parseInt(nodeParts[1]));
		IndexNode.Iface indexNode = remoteNode.getClient();
		
		indexNode.createPartition(groupId, partitionId);
		System.out.println("Partition created");
	}
}
