package ar.uba.fi.tppro.core.broker;

import java.util.Map;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;

public class BrokenPartitionException extends Exception {
	
	Map<IndexNodeDescriptor, Exception> coughtExceptions;

	public BrokenPartitionException(String string,
			Map<IndexNodeDescriptor, Exception> coughtExceptions) {
		super(string);
		
		this.coughtExceptions = coughtExceptions;
	}

}
