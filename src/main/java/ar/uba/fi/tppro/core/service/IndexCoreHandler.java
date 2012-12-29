package ar.uba.fi.tppro.core.service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.index.IndexInterface;
import ar.uba.fi.tppro.core.index.IndexPartition;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class IndexCoreHandler implements IndexInterface {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);

	private File dataPath;
	private Map<Integer, IndexPartition> partitionMap = Maps.newHashMap();

	public IndexCoreHandler(File dataPath) throws IOException {
		this.dataPath = dataPath;
	}

	protected IndexPartition getIndexPartition(int partitionId)
			throws IOException {

		// TODO: check concurrency
		if (partitionMap.containsKey(partitionId))
			return partitionMap.get(partitionId);

		File path = new File(dataPath, new Integer(partitionId).toString());

		if (!path.exists() || !path.isDirectory() || !path.canWrite()) {
			return null;
		}

		IndexPartition partition = new IndexPartition(path);
		partitionMap.put(partitionId, partition);

		return partition;
	}

	@Override
	public void deleteByQuery(int partitionId, String query) throws TException {
		logger.debug("DeleteByQuery request");
	}

	@Override
	public void index(int partitionId, List<Document> documents)
			throws TException, NonExistentPartitionException {
		logger.debug("index request");

		IndexPartition partition;
		try {
			partition = this.getIndexPartition(partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(partitionId);
		}

		try {
			partition.index(documents);
		} catch (IOException e) {
			throw new TException("Could not index in the index partition "
					+ partitionId, e);
		}
	}

	@Override
	public void createPartition(int partitionId)
			throws PartitionAlreadyExistsException, TException {
		logger.debug("createPartition request");

		File partitionPath = new File(dataPath,
				new Integer(partitionId).toString());

		if (partitionPath.exists()) {
			throw new PartitionAlreadyExistsException(partitionId);
		}

		if (!partitionPath.mkdirs()) {
			throw new TException("Could not create the partition directory");
		}
	}

	@Override
	public void removePartition(int partitionId)
			throws NonExistentPartitionException, TException {
		logger.debug("removePartition request");
	}

	@Override
	public QueryResult search(int partitionId, String query, int limit,
			int offset) throws ParseException, NonExistentPartitionException,
			TException {
		logger.debug("Search request");

		IndexPartition partition;
		try {
			partition = this.getIndexPartition(partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(partitionId);
		}

		try {
			return partition.search(partitionId, query, limit, offset);
		} catch (IOException e) {
			throw new TException("Could not search the index partition "
					+ partitionId, e);
		}
	}

	@Override
	public boolean containsPartition(int partitionId) throws TException {
		logger.debug("containsPartition request");

		File partitionPath = new File(dataPath,
				new Integer(partitionId).toString());

		return partitionPath.exists();
	}

}
