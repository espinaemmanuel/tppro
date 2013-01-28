package ar.uba.fi.tppro.core.index;

import org.apache.thrift.transport.TTransportException;

public class IndexNodeDescriptorException extends Exception {

	public IndexNodeDescriptorException(String msg, TTransportException e) {
		super(msg, e);
	}

}
