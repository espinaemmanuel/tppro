package ar.uba.fi.tppro.core.broker;

import org.apache.thrift.transport.TTransportException;

public class RemoteNodePoolException extends Exception {

	public RemoteNodePoolException(String msg, TTransportException e) {
		super(msg, e);
	}

}
