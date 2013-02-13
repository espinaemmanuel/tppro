package ar.uba.fi.tppro.core.service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.index.IndexInterface;
import ar.uba.fi.tppro.core.index.IndexPartition;
import ar.uba.fi.tppro.core.index.PartitionNotReadyException;
import ar.uba.fi.tppro.core.index.PartitionResolver;
import ar.uba.fi.tppro.core.index.IndexPartition.Status;
import ar.uba.fi.tppro.core.index.PartitionStatusObserver;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.PartitionReplicator;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.PartitionStatus;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.core.service.thrift.ReplicationException;

public class IndexCoreHandler implements IndexInterface {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);

	private File dataPath;
	private ConcurrentMap<Integer, IndexPartition> partitionMap = Maps.newConcurrentMap();
	private PartitionResolver partitionResolver;
	private LockManager lockManager;
	private PartitionStatusObserver partitionObserver;

	public IndexCoreHandler(File dataPath) {
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
		partition.setStatus(Status.READY);
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
			throw new NonExistentPartitionException(Lists.newArrayList(partitionId));
		}

		try {
			partition.index(documents);
		} catch (IOException e) {
			throw new TException("Could not index in the index partition "
					+ partitionId, e);
		} catch (PartitionNotReadyException e) {
			throw new TException("Partition not ready: " + partitionId, e);
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
		
		IndexPartition newPartition = new IndexPartition(partitionPath);
		newPartition.setStatus(Status.READY);
		
		partitionMap.put(partitionId, newPartition);

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
			throw new NonExistentPartitionException(Lists.newArrayList(partitionId));
		}

		try {
			return partition.search(partitionId, query, limit, offset);
		} catch (IOException e) {
			throw new TException("Could not search the index partition "
					+ partitionId, e);
		} catch (PartitionNotReadyException e) {
			throw new TException("Partition not ready: " + partitionId, e);
		}
	}

	@Override
	public boolean containsPartition(int partitionId) throws TException {
		logger.debug("containsPartition request");

		File partitionPath = new File(dataPath,
				new Integer(partitionId).toString());

		return partitionPath.exists();
	}

	@Override
	public void replicate(int partitionId) throws ReplicationException,
			TException {
		
		IndexPartition partition = partitionMap.get(partitionId);
		if(partition != null){
			switch(partition.getStatus()){				
			case FAILURE:
			case REPLICATION_FAILED:
				try {
					partition.clear();					
				} catch (IOException e) {
					logger.error("Could not clean partition", e);
					throw new ReplicationException(3, "Could not start the replication because there was an exception cleaning the old partition");
				}
				break;

			case READY:
				throw new ReplicationException(1, "The partition " + partitionId + " already exists");
			case REPLICATING:
				throw new ReplicationException(2, "The partition " + partitionId + " is already replicating");
			default:
				break;
			}
		} else {
			File partitionPath = new File(dataPath,
					new Integer(partitionId).toString());

			partition = new IndexPartition(partitionPath);
			partition.setStatus(Status.CREATED);
		}
		
		assert partition.getStatus() == Status.CREATED;
		
		PartitionReplicator replicator = new PartitionReplicator();
		replicator.setPartitionId(partitionId);
		replicator.setPartitionResolver(partitionResolver);
		replicator.setLockManager(lockManager);
		replicator.setStatusObserver(partitionObserver);

		partition.runReplicator(replicator);
		
	}

	@Override
	public PartitionStatus partitionStatus(int partitionId)
			throws NonExistentPartitionException, TException {
		
		IndexPartition partition = null;
		
		try {
			partition = this.getIndexPartition(partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(Lists.newArrayList(partitionId));
		}
		
		PartitionStatus status = new PartitionStatus();
		status.partitionId = partitionId;
		status.status = partition.getStatus().toString();

		return status;
	}

	@Override
	public List<String> listPartitionFiles(int partitionId)
			throws NonExistentPartitionException, TException {
		
		IndexPartition partition = null;
		
		try {
			partition = this.getIndexPartition(partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(Lists.newArrayList(partitionId));
		}
		
		try {
			return partition.listFiles();
		} catch (IOException e) {
			throw new TException("Exception retrieving file list", e);
		}
	}

	public File getDataPath() {
		return dataPath;
	}

	public void setDataPath(File dataPath) {
		this.dataPath = dataPath;
	}

	public PartitionResolver getPartitionResolver() {
		return partitionResolver;
	}

	public void setPartitionResolver(PartitionResolver partitionResolver) {
		this.partitionResolver = partitionResolver;
	}

	public LockManager getLockManager() {
		return lockManager;
	}

	public void setLockManager(LockManager lockManager) {
		this.lockManager = lockManager;
	}

	public PartitionStatusObserver getPartitionObserver() {
		return partitionObserver;
	}

	public void setPartitionObserver(PartitionStatusObserver partitionObserver) {
		this.partitionObserver = partitionObserver;
	}

}
