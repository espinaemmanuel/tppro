package ar.uba.fi.tppro.core.index;

import java.io.File;
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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.Hit;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class IndexPartition {

	private IndexWriterConfig config;
	private Directory index;
	private StandardAnalyzer analyzer;

	public IndexPartition(File path) throws IOException {
		analyzer = new StandardAnalyzer(Version.LUCENE_40);
		index = FSDirectory.open(path);
		config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
	}

	public void index(List<Document> documents) throws IOException {

		IndexWriter w;
		w = new IndexWriter(index, config);

		for (Document document : documents) {
			org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();

			for (Map.Entry<String, String> field : document.fields.entrySet()) {
				luceneDoc.add(new TextField(field.getKey(), field.getValue(),
						Field.Store.YES));
			}
			w.addDocument(luceneDoc);
		}

		w.close();

	}

	public QueryResult search(int partitionId, String query, int limit,
			int offset) throws ParseException, IOException {

		// TODO: definir el default field
		Query q;
		try {
			q = new QueryParser(Version.LUCENE_40, "text", analyzer)
					.parse(query);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			throw new ParseException(e.getMessage());
		}

		IndexReader reader = DirectoryReader.open(index);
		IndexSearcher searcher = new IndexSearcher(reader);
		TopScoreDocCollector collector = TopScoreDocCollector.create(limit,
				true);
		searcher.search(q, collector);

		ScoreDoc[] hits = collector.topDocs(offset).scoreDocs;
		
		QueryResult queryResult = new QueryResult();
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

		return queryResult;
	}

}
