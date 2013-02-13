package ar.uba.fi.tppro.core.broker;

import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;

public class IndexerException extends Exception {

	public IndexerException(String msg, Throwable e) {
		super(msg, e);
	}

}
