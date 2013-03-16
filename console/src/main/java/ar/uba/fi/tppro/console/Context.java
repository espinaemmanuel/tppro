package ar.uba.fi.tppro.console;

import java.util.List;

import com.netflix.curator.framework.CuratorFramework;

public class Context {
	
	public enum NodeType{
		BROKER, INDEX
	}

	public CuratorFramework curator;
	public List<String> indexNodes;
	public List<String> brokerNodes;
	public NodeType currentNodeType;

	public String currentNode;

}
