package ar.uba.fi.tppro.console.commands;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;
import ar.uba.fi.tppro.console.Context.NodeType;

public class UseNode implements Command {

	public String getName() {
		return "use";
	}

	public void execute(String[] argv, Context context) throws Exception {
		if (context.indexNodes == null || context.brokerNodes == null) {
			System.out.println("List nodes first");
		}
		
		int nodeNumber = Integer.parseInt(argv[1]);
		
		if(nodeNumber < context.indexNodes.size()){
			context.currentNode = context.indexNodes.get(nodeNumber);
			context.currentNodeType = NodeType.INDEX;
		} else{
			context.currentNode = context.brokerNodes.get(nodeNumber - context.indexNodes.size());
			context.currentNodeType = NodeType.BROKER;
		}
		
		System.out.println("Current node: " + context.currentNode + " " + context.currentNodeType.toString());

	}

}
