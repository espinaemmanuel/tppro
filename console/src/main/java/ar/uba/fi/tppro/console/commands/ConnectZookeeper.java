package ar.uba.fi.tppro.console.commands;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;

public class ConnectZookeeper implements Command{

	public String getName() {
		return "connect";
	}

	public void execute(String[] argv, Context context) throws Exception {
		String host = argv[1];
		String port = argv[2]; 
		
		CuratorFramework curator = 	CuratorFrameworkFactory.newClient(host + ":" + port, new RetryOneTime(1));
		curator.start();
		
		if(context.curator != null){
			context.curator.close();
		}
		
		context.curator = curator;
		
		System.out.println("Connected to zookeeper: " + host + ":" + port);
	}

	

}
