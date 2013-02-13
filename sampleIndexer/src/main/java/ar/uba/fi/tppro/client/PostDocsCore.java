package ar.uba.fi.tppro.client;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;


public class PostDocsCore extends PostDocsBase {
		
	@Override
	protected void run(String[] args) throws PartitionAlreadyExistsException, TException, JsonIOException, JsonSyntaxException, FileNotFoundException{	
		String host = System.getProperty("host", "localhost");
		String port = System.getProperty("port", "9090");
		int partition = Integer.parseInt(System.getProperty("partition", "0"));

		TTransport transport = new TSocket(host, Integer.parseInt(port));
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();
		
		if(args.length == 0){
			System.out.println("Missing files");
			return;
		}
		
		if(!client.containsPartition(partition)){
			client.createPartition(partition);
		}
		
		for(String jsonFile : args){
			List<Document> documents = createDocuments(jsonFile);
			client.index(partition, documents);
			logger.info("Indexed " + documents.size() + " documents");		
		}
	}
}
