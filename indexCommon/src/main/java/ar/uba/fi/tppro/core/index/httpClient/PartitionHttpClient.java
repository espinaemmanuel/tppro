package ar.uba.fi.tppro.core.index.httpClient;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class PartitionHttpClient {

	public String host;
	public int port;
	public int partition;
	public int group;
	
	public PartitionHttpClient(String host, int port, int groupId, int partitionId) {
		super();
		this.host = host;
		this.port = port;
		this.partition = partitionId;
		this.group = groupId;
	}
	
	protected String buildUrl(String name){
		return String.format("http://%s:%d/%d_%d/%s", host, port, group, partition, name);
	}
	
	public InputStream getFile(String name) throws PartitionHttpClientException{
		
		try {
			HttpClient httpclient = new DefaultHttpClient();
			HttpGet httpget = new HttpGet(buildUrl(name));
			HttpResponse response = httpclient.execute(httpget);
			if(response.getStatusLine().getStatusCode() != 200){
				throw new PartitionHttpClientException("HTTP Code: " + response.getStatusLine().getStatusCode());
			}
			HttpEntity entity = response.getEntity();
			
			if(entity == null){
				throw new PartitionHttpClientException("Unexpected empy file");
			}
			
			return entity.getContent();
			
		} catch (ClientProtocolException e) {
			throw new PartitionHttpClientException("Http error", e);
		} catch (IOException e) {
			throw new PartitionHttpClientException("Connection error", e);
		}

		
	}

}
