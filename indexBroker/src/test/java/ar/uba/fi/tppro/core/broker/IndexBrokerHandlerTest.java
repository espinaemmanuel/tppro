package ar.uba.fi.tppro.core.broker;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.lock.LockManager.LockType;
import ar.uba.fi.tppro.core.service.IndexCore;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParalellIndexException;
import ar.uba.fi.tppro.core.service.thrift.ParalellSearchResult;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;

public class IndexBrokerHandlerTest {
	
	protected IndexCore initServer(int port){
			File tempDir = Files.createTempDir();
			tempDir.deleteOnExit();
			
			if(tempDir.list().length > 0)
				fail("temp directory not empty");
			
			IndexCore core = new IndexCore(port, tempDir);
			
			new Thread(core).start();
			
			return core;
	}
	
	protected void createPartitions(int port, Collection<Integer> partitions) throws PartitionAlreadyExistsException, TException{
		TTransport transport = new TSocket("localhost", port);
		TProtocol protocol = new TBinaryProtocol(transport);
		IndexNode.Client client = new IndexNode.Client(protocol);
		transport.open();
		
		for(int pId : partitions){
			if(!client.containsPartition(pId)){
				client.createPartition(pId);
			}
		}

		transport.close();
	}
	
	protected Document doc(String ... fields){
		Document doc = new Document();
		doc.fields = Maps.newHashMap();		
		for(int i=0; i<fields.length; i+=2){
			doc.fields.put(fields[i], fields[i+1]);
		}
		return doc;
	}
	
	protected List<Document> createDocuments(){
		List<Document> documents = Lists.newArrayList();

		documents
				.add(doc(
						"title",
						"Charade",
						"release",
						"1963-12-05",
						"overview",
						"Romance and suspense in Paris, as a woman is pursued by several men who want a fortune her murdered husband had stolen. Who can she trust?"));
		documents
				.add(doc(
						"title",
						"Empire of the Sun",
						"release",
						"1987-12-09",
						"overview",
						"The novel recounts the story of a young English boy, Jim Graham, who lives with his parents in Shanghai. After the Pearl Harbour attack, the Japanese occupy the Shanghai International Settlement, and in the following chaos Jim becomes separated from his parents."));
		documents
				.add(doc(
						"title",
						"Winnie the Pooh",
						"release",
						"2011-07-15",
						"overview",
						"During an ordinary day in Hundred Acre Wood, Winnie the Pooh sets out to find some honey. Misinterpreting a note from Christopher Robin, Pooh convinces Tigger, Rabbit, Piglet, Owl, Kanga, Roo, and Eeyore that their young friend has been captured by a creature named \"Backson\" and they set out to save him."));
		documents
				.add(doc(
						"title",
						"The Shawshank Redemption",
						"release",
						"1994-09-14",
						"overview",
						"Framed in the 1940s for the double murder of his wife and her lover, upstanding banker Andy Dufresne begins a new life at the Shawshank prison, where he puts his accounting skills to work for an amoral warden. During his long stretch in prison, Dufresne comes to be admired by the other inmates -- including an older prisoner named Red -- for his integrity and unquenchable sense of hope."));
		documents
				.add(doc(
						"title",
						"The Godfather",
						"release",
						"1972-03-15",
						"overview",
						"The story spans the years from 1945 to 1955 and chronicles the fictional Italian-American Corleone crime family. When organized crime family patriarch Vito Corleone barely survives an attempt on his life, his youngest son, Michael, steps in to take care of the would-be killers, launching a campaign of bloody revenge."));
		documents
				.add(doc(
						"title",
						"The Good, the Bad and the Ugly",
						"release",
						"1966-12-23",
						"overview",
						"While the Civil War rages between the Union and the Confederacy, three men -- a quiet loner, a ruthless hit man and a Mexican bandit -- comb the American Southwest in search of a strongbox containing $200,000 in stolen gold."));
		documents
				.add(doc(
						"title",
						"Once Upon a Time in America",
						"release",
						"1984-02-17",
						"overview",
						"This Mafia film is the greatest and last work from Italian director Sergio Leone. Taking place in 1920 to 1960 America the film follows a group Jewish gangsters from childhood into their glory years of the prohibition and as they reunite in their later years."));
		documents
				.add(doc(
						"title",
						"The Matrix",
						"release",
						"1999-06-17",
						"overview",
						"Thomas A. Anderson is a man living two lives. By day he is an average computer programmer and by night a malevolent hacker known as Neo, who finds himself targeted by the police when he is contacted by Morpheus, a legendary computer hacker, who reveals the shocking truth about our reality."));
		documents
				.add(doc(
						"title",
						"Fight Club",
						"release",
						"1999-10-15",
						"overview",
						"A ticking-time-bomb insomniac and a slippery soap salesman channel primal male aggression into a shocking new form of therapy. Their concept catches on, with underground \"fight clubs\" forming in every town, until an eccentric gets in the way and ignites an out-of-control spiral toward oblivion."));
		documents
				.add(doc(
						"title",
						"Blade Runner",
						"release",
						"1982-06-25",
						"overview",
						"In the smog-choked dystopian Los Angeles of 2019, blade runner Rick Deckard is called out of retirement to snuff a quartet of replicants, who have escaped to Earth seeking their creator for a way to extend their short life spans."));
		documents
				.add(doc(
						"title",
						"The Lord of the Rings, The Return of the King",
						"release",
						"2003-12-17",
						"overview",
						"Aragorn is revealed as the heir to the ancient kings as he, Gandalf and the other members of the broken fellowship struggle to save Gondor from Sauron\u0027s forces. Meanwhile, Frodo and Sam bring the ring closer to the heart of Mordor, the dark lord\u0027s realm."));
		documents
				.add(doc(
						"title",
						"Star Wars, Episode V - The Empire Strikes Back",
						"release",
						"1980-05-21",
						"overview",
						"The epic saga continues as Luke Skywalker, in hopes of defeating the evil Galactic Empire, learns the ways of the Jedi from aging master Yoda. But Darth Vader is more determined than ever to capture Luke. Meanwhile, rebel leader Princess Leia, cocky Han Solo, Chewbacca, and droids C-3PO and R2-D2 are thrown into various stages of capture, betrayal and despair."));

		return documents;
	}
	
	

	@Test
	public void testIndexAndSearch() throws LockAquireTimeoutException, ParalellIndexException, NonExistentPartitionException, TException, InterruptedException {
		
		IndexCore core1 = initServer(9001);
		IndexCore core2 = initServer(9002);
		IndexCore core3 = initServer(9003);
		Thread.sleep(5000);
		
		Multimap<Integer, Integer> parts = LinkedListMultimap.create();

		parts.put(9001, 2);
		parts.put(9001, 3);
		parts.put(9002, 1);
		parts.put(9002, 3);
		parts.put(9003, 1);
		parts.put(9003, 2);
		
		for(int pId : parts.keySet()){
			createPartitions(pId, parts.get(pId));
		}
		
		RemoteNodePool nodePool = new RemoteNodePool();
		StaticSocketPartitionResolver resolver = new StaticSocketPartitionResolver(nodePool);
		resolver.addReplica("localhost", 9001, 2);
		resolver.addReplica("localhost", 9001, 3);
		resolver.addReplica("localhost", 9002, 1);
		resolver.addReplica("localhost", 9002, 3);
		resolver.addReplica("localhost", 9003, 1);
		resolver.addReplica("localhost", 9003, 2);
		
		LockManager lockManager = mock(LockManager.class);
		IndexLock indexLock = mock(IndexLock.class);
		when(lockManager.aquire(eq(LockType.ADD), anyInt())).thenReturn(indexLock);
		
		IndexBrokerHandler broker = new IndexBrokerHandler(resolver, lockManager);
		
		broker.index(Lists.newArrayList(1, 2, 3), createDocuments());
		
		ParalellSearchResult result = broker.search(Lists.newArrayList(1, 2, 3), "*:*", 10, 0);
		assertEquals(12, result.qr.totalHits);
		
		result = broker.search(Lists.newArrayList(1, 2, 3), "title:Blade OR title:Star", 10, 0);
		assertEquals(2, result.qr.totalHits);
		
		broker.index(Lists.newArrayList(1, 2), createDocuments());
		
		result = broker.search(Lists.newArrayList(1, 2, 3), "*:*", 10, 0);
		assertEquals(24, result.qr.totalHits);
		
		result = broker.search(Lists.newArrayList(1, 2), "title:Blade OR title:Star", 10, 0);
		assertEquals(3, result.qr.totalHits);

		core1.stop();
		Thread.sleep(5000);
		
		result = broker.search(Lists.newArrayList(1, 2), "title:Blade OR title:Star", 10, 0);
		assertEquals(3, result.qr.totalHits);

	}

}
