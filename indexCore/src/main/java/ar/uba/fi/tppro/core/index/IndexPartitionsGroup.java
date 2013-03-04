package ar.uba.fi.tppro.core.index;

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
import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.PartitionVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.PartitionStatus;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.core.service.thrift.ReplicationException;
import ar.uba.fi.tppro.partition.PartitionResolver;
import ar.uba.fi.tppro.partition.PartitionResolverException;


/**
 * A node that contains several partitions
 * 
 * @author emmanuelespina
 *
 */
public class IndexPartitionsGroup implements IndexInterface, PartitionStatusObserver {

	final Logger logger = LoggerFactory.getLogger(IndexPartitionsGroup.class);

	private File dataPath;
	private ConcurrentMap<Integer, IndexPartition> partitionMap = Maps.newConcurrentMap();
	private PartitionResolver partitionResolver;
	private LockManager lockManager;
	private IndexNodeDescriptor thisNodeDescriptor;
	private PartitionVersionTracker versionTracker;

	public IndexPartitionsGroup(IndexNodeDescriptor thisNode, PartitionResolver partitionResolver, PartitionVersionTracker versionTracker) {
		this.thisNodeDescriptor = thisNode;
		this.partitionResolver = partitionResolver;
		this.versionTracker = versionTracker;
	}
	
	public void open(File dataPath){
		this.dataPath = dataPath;

		for(File partitionDir : dataPath.listFiles()){

			if (!partitionDir.isDirectory()) {
				continue;
			}

			int partitionId = Integer.parseInt(partitionDir.getName());
			IndexPartition partition = new IndexPartition(partitionId, partitionDir, this.versionTracker);
			
			try {
				
				partition.open();
				partitionMap.put(partitionId, partition);

			} catch (IOException e) {
				logger.error("Could not open partition " + partitionId, e);
			}
		}
		
		//Check and replicate failed partitions
		for(IndexPartition partition : partitionMap.values()){
			if(partition.getStatus() == IndexPartitionStatus.RESTORING){
				PartitionReplicator replicator = new PartitionReplicator();
				replicator.setPartitionResolver(partitionResolver);
				replicator.setLockManager(lockManager);
				replicator.setStatusObserver(this);

				partition.runReplicator(replicator);
			}
		}
	}

	protected IndexPartition getIndexPartition(int partitionId)
			throws IOException {
			return partitionMap.get(partitionId);
	}

	@Override
	public void deleteByQuery(int partitionId, String query) throws TException {
		logger.debug("DeleteByQuery request");
		throw new UnsupportedOperationException("delete Not yet implemented");
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
	/**
	 * Creates a partition and tries to replicate it from another node. If the partition already exists
	 */
	public void createPartition(int partitionId)
			throws TException {
		logger.debug("createPartition request");

		File partitionPath = new File(dataPath,
				new Integer(partitionId).toString());

		if (partitionPath.exists()) {
			throw new PartitionAlreadyExistsException();
		}

		if (!partitionPath.mkdirs()) {
			throw new TException("Could not create the partition directory");
		}
		
		IndexPartition newPartition = new IndexPartition(partitionId, partitionPath, this.versionTracker);
		partitionMap.put(partitionId, newPartition);
		
		try {
			newPartition.open();
		} catch (IOException e) {
			throw new TException("Could not open the partition", e);
		}
		
		try {
			this.partitionResolver.registerPartition(partitionId, this.getThisNodeDescriptor(), newPartition.getStatus());
		} catch (PartitionResolverException e) {
			partitionPath.delete();
			partitionMap.remove(partitionId);
			throw new TException("Coudl not register the new partition");
		}
		
		//If the partition is stale, replicate it
		if(newPartition.getStatus() == IndexPartitionStatus.RESTORING){
			if(newPartition.getStatus() == IndexPartitionStatus.RESTORING){
				PartitionReplicator replicator = new PartitionReplicator();
				replicator.setPartitionResolver(partitionResolver);
				replicator.setLockManager(lockManager);
				replicator.setStatusObserver(this);

				newPartition.runReplicator(replicator);
			}
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
		
		return partitionMap.containsKey(partitionId);

	}

	@Override
	public void replicate(int partitionId) throws ReplicationException,
			TException {
		
		IndexPartition partition = partitionMap.get(partitionId);
		
		if(partition == null){
			//Partition does not exists. Create a new one and replicate
			this.createPartition(partitionId);
			return;
		}
		
		//The partition already exists and must be replicated from another node
		if(partition != null){
			switch(partition.getStatus()){				
			case FAILURE:
			case RESTORE_FAILED:
				try {
					partition.clear();					
				} catch (IOException e) {
					logger.error("Could not clean partition", e);
					throw new ReplicationException(3, "Could not start the replication because there was an exception cleaning the old partition");
				}
				break;

			case READY:
				throw new ReplicationException(1, "The partition " + partitionId + " already exists");
			case RESTORING:
				throw new ReplicationException(2, "The partition " + partitionId + " is already replicating");
			default:
				break;
			}
		}
				
		PartitionReplicator replicator = new PartitionReplicator();
		replicator.setPartitionResolver(partitionResolver);
		replicator.setLockManager(lockManager);
		replicator.setStatusObserver(this);

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

	public IndexNodeDescriptor getThisNodeDescriptor() {
		return thisNodeDescriptor;
	}

	public void setThisNodeDescriptor(IndexNodeDescriptor thisNodeDescriptor) {
		this.thisNodeDescriptor = thisNodeDescriptor;
	}

	@Override
	public void notifyStatusChange(IndexPartition indexPartition) {
		// TODO Auto-generated method stub		
	}

	public PartitionVersionTracker getVersionTracker() {
		return versionTracker;
	}

	public void setVersionTracker(PartitionVersionTracker versionTracker) {
		this.versionTracker = versionTracker;
	}

}
