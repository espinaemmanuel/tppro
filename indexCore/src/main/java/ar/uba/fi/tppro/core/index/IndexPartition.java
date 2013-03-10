package ar.uba.fi.tppro.core.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.index.versionTracker.PartitionVersionObserver;
import ar.uba.fi.tppro.core.index.versionTracker.PartitionVersionTracker;
import ar.uba.fi.tppro.core.index.versionTracker.VersionTrackerServerException;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class IndexPartition implements Closeable, PartitionVersionObserver {

	final Logger logger = LoggerFactory.getLogger(IndexPartition.class);

	private static final String DEFAULT_FIELD = "text";
	private static final String INDEX_VERSION = "indexVersion";
	
	private File dataPath;
	private String defaultField = DEFAULT_FIELD;
	private IndexWriterConfig config;
	private Directory indexDir;
	private StandardAnalyzer analyzer;
	private SearcherManager mgr;
	boolean isOpen = false;
	private PartitionVersionTracker versionTracker;
	
	private int partitionId;
	
	/*
	 * Two phase commit implementation
	 */
	private Integer lastCommittedMessageId;
	private Integer lastPreparedMessageId;

	
	private IndexWriter currentWriter;
	
	private IndexPartitionStatus status;

	private String lastError;

	public IndexPartition(int partitionId, File dataPath, PartitionVersionTracker versionTracker) {
		this.partitionId = partitionId;
		this.dataPath = dataPath;
		this.versionTracker = versionTracker;
		
		this.status = IndexPartitionStatus.CREATED;
		this.analyzer = new StandardAnalyzer(Version.LUCENE_40);
	}
	
	public void open() throws IOException{
					
		indexDir = FSDirectory.open(dataPath);
		config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		
		//If the directory is empty, create an empty index
		if(dataPath.list().length == 0){
			IndexWriter initialWriter = new IndexWriter(indexDir, config);
			initialWriter.close();
		}
		
		int clusterVersion;
		try {
			
			clusterVersion = this.versionTracker.getCurrentVersion(this.partitionId);
			this.versionTracker.addVersionObserver(this.partitionId, this);
			
		} catch (VersionTrackerServerException e) {
			throw new IOException("could not retrieve version from server", e);
		}
		
		this.lastCommittedMessageId = this.readCurrentVersion();
		
		if(clusterVersion == this.lastCommittedMessageId){
			logger.info("Partition up to date");
			this.status = IndexPartitionStatus.READY;
		} else {
			logger.info(String.format("Index stale - Local version = %d, Cluster version = %d. Will eventally restore", this.lastCommittedMessageId, clusterVersion));
			this.status = IndexPartitionStatus.RESTORING;
		}
			
		mgr = new SearcherManager(indexDir, new SearcherFactory());	
		isOpen = true;
	}
	
	private int readCurrentVersion() throws IOException {
		
		//Get a list of commits of the directory
		SegmentInfos segmentInfos = new SegmentInfos();
		segmentInfos.read(this.indexDir);
		long indexGeneration = segmentInfos.getLastGeneration();
		
		assert indexGeneration > 0 : "index generation cannot be less than 0";
		
		if(indexGeneration == 1){
			return 0;
		}
		
		IndexCommit currentCommit = null;
		
		for(IndexCommit commit : DirectoryReader.listCommits(indexDir)){
			if(commit.getGeneration() == indexGeneration){
				//This is the current generation commit
				currentCommit = commit;
				break;				
			}
		}
		
		if(currentCommit == null)
			throw new IOException("Could not get current commit");
		
		return Integer.parseInt(currentCommit.getUserData().get(INDEX_VERSION));
	}

	protected void ensureOpen() throws IOException{
		if(!isOpen){
			this.open();
		}
	}

	/**
	 * Single phase commit. Actually performs two phase commit internally
	 * 
	 * @param documents
	 * @throws IOException
	 * @throws PartitionNotReadyException
	 */
	public void index(List<Document> documents) throws IOException, PartitionNotReadyException {
		int messageId = this.lastPreparedMessageId != null ? this.lastPreparedMessageId + 1 : this.lastCommittedMessageId + 1;
			
		this.prepare(messageId, documents);
		this.commit();	
	}

	/**
	 * Phase 1 of the two phase commit
	 * 
	 * @param documents
	 * @throws IOException
	 * @throws PartitionNotReadyException
	 */
	public void prepare(int messageId, List<Document> documents) throws IOException, PartitionNotReadyException {
		
		if(this.status != IndexPartitionStatus.READY){
			throw new PartitionNotReadyException("partition not ready: " + this.status);
		}
		
		ensureOpen();
		long startTime = System.currentTimeMillis();

		if(this.currentWriter == null){
			this.currentWriter = new IndexWriter(indexDir, config);
		}	
		
		//lastPrepared = messageId -> discard last prepared
		if(this.lastPreparedMessageId != null && this.lastPreparedMessageId == messageId){
			this.currentWriter.rollback();			
			this.currentWriter = new IndexWriter(indexDir, config);			
			this.lastPreparedMessageId = null;
		}
		
		//lost messages
		if(this.lastPreparedMessageId != null && messageId - this.lastPreparedMessageId > 1 ||
		   this.lastPreparedMessageId == null && messageId - this.lastCommittedMessageId > 1){
			this.status = IndexPartitionStatus.RESTORING;
			throw new PartitionNotReadyException("stale partition. Will eventually restore");
		}
		
		//Correct order but missed some commits
		if(this.lastPreparedMessageId != null && messageId == this.lastPreparedMessageId + 1){
			this.commit();
		}
		
		for (Document document : documents) {
			org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

			for (Map.Entry<String, String> field : document.fields.entrySet()) {
				luceneDoc.add(new TextField(field.getKey(), field.getValue(),
						Field.Store.YES));
			}
			
			this.currentWriter.addDocument(luceneDoc);
		}
		
		assert this.lastCommittedMessageId == null : "last committed message Id should be null";
		
		Map<String, String> userDataMap = Maps.newConcurrentMap();
		userDataMap.put(INDEX_VERSION, Integer.toString(messageId));
		this.lastPreparedMessageId = messageId;
		
		this.currentWriter.prepareCommit(userDataMap);
		
		long endTime = System.currentTimeMillis();	
		logger.debug(String.format("PREPARED (id: %d): DocCount=%d IndexTime=%d", this.lastPreparedMessageId, documents.size(), endTime - startTime));
	}
	
	/**
	 * Phase 2 of two phase commit
	 * 
	 * @throws IOException
	 */
	public void commit() throws IOException{
		Preconditions.checkNotNull(this.currentWriter);
		
		if(this.lastPreparedMessageId != null){
			this.currentWriter.commit();
			
			this.lastCommittedMessageId = this.lastPreparedMessageId;
			this.lastPreparedMessageId = null;
			
			logger.debug(String.format("COMMITED (id: %d)", this.lastCommittedMessageId));
			
			this.currentWriter.close();
			this.currentWriter = null;
			
			mgr.maybeRefresh();
			
		}
	}


	public QueryResult search(int partitionId, String query, int limit,
			int offset) throws ParseException, IOException, PartitionNotReadyException {
		
		if(this.status != IndexPartitionStatus.READY){
			throw new PartitionNotReadyException("partition not ready: " + this.status);
		}
		
		ensureOpen();

		long startTime = System.currentTimeMillis();

		Query q;
		try {
			q = new QueryParser(Version.LUCENE_40, defaultField, analyzer)
					.parse(query);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			throw new ParseException(e.getMessage());
		}

		IndexSearcher searcher = mgr.acquire();
		QueryResult queryResult = new QueryResult();

		try {

			TopScoreDocCollector collector = TopScoreDocCollector.create(limit,
					true);

			searcher.search(q, collector);

			ScoreDoc[] hits = collector.topDocs(offset).scoreDocs;

			queryResult.hits = Lists.newArrayList();
			queryResult.parsedQuery = q.toString();
			queryResult.totalHits = collector.getTotalHits();

			for (int i = 0; i < hits.length; i++) {
				int docId = hits[i].doc;
				float score = hits[i].score;

				org.apache.lucene.document.Document d = searcher.doc(docId);

				Hit hit = new Hit();
				hit.doc = new Document();
				hit.doc.fields = Maps.newHashMap();
				hit.score = score;

				for (IndexableField field : d.getFields()) {
					if (field.fieldType().stored()) {
						String name = field.name();
						String val = field.stringValue();

						hit.doc.fields.put(name, val);
					}
				}

				queryResult.hits.add(hit);
			}

		} finally {
			mgr.release(searcher);
		}
		
		long endTime = System.currentTimeMillis();
		logger.debug(String.format("QUERY: Partition=%d query=[%s] limit=%d offset=%d -> qtime=%d hits=%d", partitionId, query, limit, offset, endTime - startTime, queryResult.totalHits));

		return queryResult;

	}

	public String getDefaultField() {
		return defaultField;
	}

	public void setDefaultField(String defaultField) {
		this.defaultField = defaultField;
	}
	
	public IndexPartitionStatus getStatus() {
		return status;
	}

	public void setStatus(IndexPartitionStatus status) {
		this.status = status;
	}

	public void clear() throws IOException {
		
		if(this.currentWriter == null){
			this.currentWriter = new IndexWriter(indexDir, config);
		}
		
		this.currentWriter.deleteAll();	
	}

	public void runReplicator(PartitionReplicator replicator) {
		replicator.setPartition(this);
		replicator.start();		
	}

	public List<String> listFiles() throws IOException {
		if(this.status != IndexPartitionStatus.READY){
			return Lists.newArrayList();
		}
		
		ensureOpen();
		
		return Lists.newArrayList(indexDir.listAll());
	}
	
	public File getDataPath() {
		return dataPath;
	}

	public void setDataPath(File dataPath) {
		this.dataPath = dataPath;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public String getLastError() {
		return lastError;
	}

	public void setLastError(String lastError) {
		this.lastError = lastError;
	}

	public void reload() throws IOException {
		this.close();
		this.open();
	}

	@Override
	public void close() throws IOException {
		if(this.currentWriter != null){
			this.currentWriter.rollback();
			this.currentWriter = null;
		}
		
		this.mgr.close();
		this.indexDir.close();
	}

	@Override
	public void onConectionLoss(int partitionId) {
		logger.error("This partition was disconnected from the cluster");
		this.status = IndexPartitionStatus.DISCONNECTED;		
	}

	@Override
	public void onVersionChanged(int partitionId, int newVersion) {
		if(this.lastPreparedMessageId != null  && this.lastPreparedMessageId == newVersion){
			try {
				this.commit();
			} catch (IOException e) {
				logger.error("Could not commit index", e);
			}		
		}
	}

}
