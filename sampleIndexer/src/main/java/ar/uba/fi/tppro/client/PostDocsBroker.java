package ar.uba.fi.tppro.client;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;

import com.google.common.collect.Lists;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class PostDocsBroker extends PostDocsBase {
	
	@Override
	protected void run(String[] args) throws PartitionAlreadyExistsException, TException, JsonIOException, JsonSyntaxException, FileNotFoundException{	
		String host = System.getProperty("host", "localhost");
		String port = System.getProperty("port", "9090");

		TTransport transport = new TSocket(host, Integer.parseInt(port));
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexBroker.Client client = new IndexBroker.Client(protocol);
		transport.open();
		
		if(args.length == 0){
			System.out.println("Missing files");
			return;
		}
		
		for(String jsonFile : args){
			List<Document> documents = createDocuments(jsonFile);
			client.index(1, documents);
			logger.info("Indexed " + documents.size() + " documents");		
		}
	}
}
