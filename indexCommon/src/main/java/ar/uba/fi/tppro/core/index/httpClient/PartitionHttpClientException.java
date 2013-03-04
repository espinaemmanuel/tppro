package ar.uba.fi.tppro.core.index.httpClient;

public class PartitionHttpClientException extends Exception {

	public PartitionHttpClientException(String msg, Throwable e) {
		super(msg, e);
	}

	public PartitionHttpClientException(String msg) {
		super(msg);
	}

}
