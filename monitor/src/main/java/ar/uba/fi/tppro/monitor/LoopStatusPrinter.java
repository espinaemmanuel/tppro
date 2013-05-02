package ar.uba.fi.tppro.monitor;

import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.uba.fi.tppro.core.service.thrift.Node;
import ar.uba.fi.tppro.core.service.thrift.NodePartition;

public class LoopStatusPrinter implements Runnable {
	
	final Logger logger = LoggerFactory.getLogger(LoopStatusPrinter.class);

	private MonitorHandler handler;
	
	public LoopStatusPrinter(MonitorHandler handler){
		this.handler = handler;
	}
	
	public void run() {
		try {
			while(true){
				System.out.println("Dumping data from zookeeper");
				
				System.out.println("NODES:");
				for(Node node : handler.getNodes()){
					System.out.println("\t" + node.url + " - " + node.type);
				}
				
				System.out.println("VERSIONS:");
				for(Map.Entry<Integer, String> groupVersion : handler.getGroupVersion().entrySet()){
					System.out.println(String.format("\tgroup: %d - version: %s", groupVersion.getKey(), groupVersion.getValue()));
				}
				
				System.out.println("REPLICAS:");
				for(NodePartition replica : handler.getReplicas()){
					System.out.println(String.format("\tgroup: %d - partition: %d - url: %s - status: %s", replica.groupId, replica.partitionId, replica.nodeUrl, replica.status));
				}
				
				Thread.sleep(5000);
			}
	
		} catch (TException e) {
			logger.error("error getting data from zookeeper", e);
		} catch (InterruptedException e) {
			logger.error("Dump loop interrupted", e);
		}
		
	}

}
