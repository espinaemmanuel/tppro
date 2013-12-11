package tests;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptorException;
import ar.uba.fi.tppro.core.index.RemoteBrokerNodeDescriptor;
import ar.uba.fi.tppro.core.service.thrift.IndexBroker;
import tests.ReutersSaxParser.ReutersDoc;
import tests.ReutersSaxParser.ReutersDocHandler;

public class IndexReuters {
	
	public static int counter = 0;


	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, IndexNodeDescriptorException {
		
		Properties properties = new Properties();
		properties.load(new FileReader("config.properties"));
		
		String brokerHost = properties.getProperty("host");
		int port = Integer.parseInt(properties.getProperty("port"));
		int bufferSize = Integer.parseInt(properties.getProperty("bufferSize"));
		int shard = Integer.parseInt(properties.getProperty("shard"));

		RemoteBrokerNodeDescriptor remoteNode = new RemoteBrokerNodeDescriptor(
				brokerHost, port);
		final IndexBroker.Iface broker = remoteNode.getClient();
		
		IndexingReutersDocHandler docHandler = new IndexingReutersDocHandler(broker, bufferSize, shard);
		ReutersSaxParser parser = new ReutersSaxParser(docHandler);

		for(String file : args){
			parser.parse(new File(file));
			System.out.println("Processed documents: " + counter);
		}
		
		System.out.println("Total time: " + docHandler.getIndexTime());		
	}

}
