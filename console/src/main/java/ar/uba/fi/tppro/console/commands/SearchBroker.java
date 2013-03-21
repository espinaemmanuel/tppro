package ar.uba.fi.tppro.console.commands;

import java.util.Map;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;
import ar.uba.fi.tppro.console.Context.NodeType;
import ar.uba.fi.tppro.core.index.RemoteBrokerNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.Error;
import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;

public class SearchBroker implements Command {

	public String getName() {
		return "search";
	}

	public void execute(String[] argv, Context context) throws Exception {
		if (context.currentNode == null) {
			System.out.println("Current node not selected");
			return;
		}
		
		if(context.currentNodeType != NodeType.BROKER){
			System.out.println("Current node is not a broker");
			return;
		}

		String[] nodeParts = context.currentNode.split("_");

		RemoteBrokerNodeDescriptor remoteNode = new RemoteBrokerNodeDescriptor(
				nodeParts[0], Integer.parseInt(nodeParts[1]));
		IndexBroker.Iface broker = remoteNode.getClient();
		
		int groupId = Integer.parseInt(argv[1]);
		String query = argv[2];
		
		ParalellSearchResult result = broker.search(groupId, query, 1000, 0);
		
		System.out.println("Errors:");
		
		for(Error error : result.errors){
			System.out.println(error.code + ": " + error.desc);
		}
		
		System.out.println("Total results: " + result.getQr().totalHits);
		System.out.println("Hits:");

		for(Hit hit : result.qr.hits){
			for(Map.Entry<String, String> entry : hit.doc.fields.entrySet()){
				System.out.println("\t" + entry.getKey() + " : " + entry.getValue());
			}
			System.out.println();
		}

	}

}
