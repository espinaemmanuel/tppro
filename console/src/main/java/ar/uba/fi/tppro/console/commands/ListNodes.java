package ar.uba.fi.tppro.console.commands;

import java.util.ArrayList;
import java.util.List;

import com.netflix.curator.framework.imps.CuratorFrameworkState;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;

public class ListNodes implements Command {

	public String getName() {
		return "list";
	}

	public void execute(String[] argv, Context context) throws Exception{
		if(context.curator == null || context.curator.getState() != CuratorFrameworkState.STARTED){
			System.out.println("Not connected to any server");
		}
		
		List<String> rootDir = context.curator.getChildren().forPath("/");
		
		context.indexNodes = rootDir.contains("cluster") ? context.curator.getChildren().forPath("/cluster") : new ArrayList<String>();
		context.brokerNodes = rootDir.contains("brokers") ?context.curator.getChildren().forPath("/brokers") : new ArrayList<String>();

		context.currentNode = null;
		context.currentNodeType = null;
		
		int counter = 0;
		
		for(int i=0; i < context.indexNodes.size(); i++){
			System.out.println(counter + ") " + context.indexNodes.get(i).replace("_", ":") + " INDEX");
			counter++;
		}
		
		for(int i=0; i < context.brokerNodes.size(); i++){
			System.out.println(counter + ") " + context.brokerNodes.get(i).replace("_", ":") + " BROKER");
			counter++;
		}
	}

}
