package ar.uba.fi.tppro.core.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class IndexCoreHandler implements IndexNode.Iface {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);

	private IndexWriterConfig config;
	private Directory index;
	private StandardAnalyzer analyzer;

	public IndexCoreHandler() {
		analyzer = new StandardAnalyzer(Version.LUCENE_40);
		index = new RAMDirectory();
		config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
	}

	@Override
	public void deleteByQuery(int partitionId, String query) throws TException {
		logger.debug("DeleteByQuery request");
	}

	@Override
	public void index(int partitionId, List<Document> documents)
			throws TException {

		logger.debug("index request");

		IndexWriter w;
		try {
			w = new IndexWriter(index, config);
		} catch (IOException e) {
			throw new TException("Could not open index writer", e);
		}

		for (Document document : documents) {
			org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

			for (Map.Entry<String, String> field : document.fields.entrySet()) {
				luceneDoc.add(new TextField(field.getKey(), field.getValue(),
						Field.Store.YES));
			}

			try {
				w.addDocument(luceneDoc);
			} catch (IOException e) {
				throw new TException("Could not add document to index", e);
			}
		}

		try {
			w.close();
		} catch (IOException e) {
			throw new TException("Could not close the index", e);
		}
	}

	@Override
	public void createPartition(int partitionId) throws TException {
		logger.debug("createPartition request");
	}

	@Override
	public void removePartition(int partitionId) throws TException {
		logger.debug("removePartition request");
	}

	@Override
	public List<QueryResult> search(int partitionId, String query, int limit,
			int offset) throws ParseException, TException {
		logger.debug("Search request");

		// TODO: definir el default field
		Query q;
		try {
			q = new QueryParser(Version.LUCENE_40, "text", analyzer)
					.parse(query);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			throw new ParseException(e.getMessage());
		}

		IndexReader reader;
		TopScoreDocCollector collector;
		IndexSearcher searcher;
		try {
			reader = DirectoryReader.open(index);
			searcher = new IndexSearcher(reader);
			collector = TopScoreDocCollector.create(limit, true);
			searcher.search(q, collector);
		} catch (IOException e) {
			throw new TException("Could not open index searcher", e);
		}

		ScoreDoc[] hits = collector.topDocs(offset).scoreDocs;

		List<QueryResult> toReturn = Lists.newArrayList();

		for (int i = 0; i < hits.length; i++) {
			int docId = hits[i].doc;
			float score = hits[i].score;

			try {
				org.apache.lucene.document.Document d = searcher.doc(docId);

				QueryResult hit = new QueryResult();
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

				toReturn.add(hit);

			} catch (IOException e) {
				throw new TException("Could not retrieve document", e);
			}
		}

		return toReturn;
	}

}
