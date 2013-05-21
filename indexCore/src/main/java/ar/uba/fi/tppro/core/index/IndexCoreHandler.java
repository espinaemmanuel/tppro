package ar.uba.fi.tppro.core.index;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ar.uba.fi.tppro.core.index.IndexInterface;
import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexException;
import ar.uba.fi.tppro.core.service.thrift.MessageId;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.PartitionStatus;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;
import ar.uba.fi.tppro.partition.PartitionResolver;
import ar.uba.fi.tppro.partition.PartitionResolverException;

/**
 * A node that contains several partitions
 * 
 * @author emmanuelespina
 * 
 */
public class IndexCoreHandler implements IndexInterface,
		PartitionStatusObserver {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);

	private File dataPath;
	private ConcurrentMap<PartitionIdentifier, IndexPartition> partitionMap = Maps
			.newConcurrentMap();
	private PartitionResolver partitionResolver;
	private LockManager lockManager;
	private IndexNodeDescriptor thisNodeDescriptor;
	private GroupVersionTracker versionTracker;

	private int simulationDelay;

	public IndexCoreHandler(IndexNodeDescriptor thisNode,
			PartitionResolver partitionResolver,
			GroupVersionTracker versionTracker, LockManager lockManager) {
		this.thisNodeDescriptor = thisNode;
		this.partitionResolver = partitionResolver;
		this.versionTracker = versionTracker;
		this.lockManager = lockManager;
	}

	public void open(File dataPath, boolean checkVersions) {
		this.dataPath = dataPath;

		Preconditions.checkArgument(this.dataPath.exists(),
				"data dir %s does not exists", this.dataPath);

		for (File partitionDir : dataPath.listFiles()) {

			if (!partitionDir.isDirectory()) {
				continue;
			}

			String[] nameParts = partitionDir.getName().split("_");

			int groupId = Integer.parseInt(nameParts[0]);
			int partitionId = Integer.parseInt(nameParts[1]);

			logger.debug(String
					.format("existing partition found in data dir: group=%d partition=%d",
							groupId, partitionId));

			IndexPartition partition = new IndexPartition(groupId, partitionId,
					partitionDir, this.versionTracker);

			try {
				this.partitionResolver.registerPartition(
						partition.getShardId(), partition.getPartitionId(),
						this.getThisNodeDescriptor(), partition.getStatus());
			} catch (PartitionResolverException e) {
				logger.error(
						String.format(
								"could not register partition. omitting: group=%d partition=%d",
								groupId, partitionId), e);
				continue;
			}

			try {
				partition.open(checkVersions);
			} catch (IOException e) {
				logger.error("Could not open partition " + partition, e);
				continue;
			}

			partitionMap.put(new PartitionIdentifier(groupId, partitionId),
					partition);
			logger.debug(String
					.format("partition opened and registered: group=%d partition=%d status=%s",
							groupId, partitionId, partition.getStatus()));

			if (partition.getStatus() != IndexPartitionStatus.CREATED) {
				try {
					this.partitionResolver.updatePartitionStatus(groupId,
							partitionId, this.getThisNodeDescriptor(),
							partition.getStatus());
				} catch (PartitionResolverException e) {
					logger.info("Could not update zookeper status", e);
				}
			}
		}

		// Check and replicate failed partitions
		for (IndexPartition partition : partitionMap.values()) {
			if (partition.getStatus() == IndexPartitionStatus.RESTORING) {
				logger.debug("Scheduling replication of partition " + partition);
				
				PartitionReplicator replicator = new PartitionReplicator();
				replicator.setPartitionResolver(partitionResolver);
				replicator.setLockManager(lockManager);
				replicator.setStatusObserver(this);

				partition.runReplicator(replicator);
			}
		}
	}

	protected IndexPartition getIndexPartition(int shardId, int partitionId)
			throws IOException {
		return partitionMap.get(new PartitionIdentifier(shardId, partitionId));
	}

	@Override
	public void deleteByQuery(int shardId, int partitionId, String query)
			throws TException {
		logger.debug("DeleteByQuery request");
		throw new UnsupportedOperationException("delete Not yet implemented");
	}

	@Override
	/**
	 * Creates a partition and tries to replicate it from another node. If the partition already exists
	 */
	public void createPartition(int shardId, int partitionId) throws TException {
		logger.debug("createPartition request");

		File partitionPath = getPartitionPath(shardId, partitionId);
		if (partitionPath.exists()) {
			throw new PartitionAlreadyExistsException();
		}

		if (!partitionPath.mkdirs()) {
			throw new TException("Could not create the partition directory");
		}

		IndexPartition newPartition = new IndexPartition(shardId, partitionId,
				partitionPath, this.versionTracker);
		partitionMap.put(new PartitionIdentifier(shardId, partitionId),
				newPartition);

		try {
			newPartition.open(true);
		} catch (IOException e) {
			throw new TException("Could not open the partition", e);
		}

		try {
			this.partitionResolver.registerPartition(shardId, partitionId,
					this.getThisNodeDescriptor(), newPartition.getStatus());
		} catch (PartitionResolverException e) {
			partitionPath.delete();
			partitionMap.remove(partitionId);
			throw new TException("Coudl not register the new partition");
		}

		// If the partition is stale, replicate it
		if (newPartition.getStatus() == IndexPartitionStatus.RESTORING) {
			if (newPartition.getStatus() == IndexPartitionStatus.RESTORING) {
				PartitionReplicator replicator = new PartitionReplicator();
				replicator.setPartitionResolver(partitionResolver);
				replicator.setLockManager(lockManager);
				replicator.setStatusObserver(this);

				newPartition.runReplicator(replicator);
			}
		}
	}

	private File getPartitionPath(int shardId, int partitionId) {
		Preconditions.checkNotNull(this.dataPath);

		return new File(dataPath, Integer.toString(shardId) + "_"
				+ Integer.toString(partitionId));
	}

	@Override
	public void removePartition(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {
		logger.debug("removePartition request");
	}

	@Override
	public QueryResult search(int shardId, int partitionId, String query,
			int limit, int offset) throws ParseException,
			NonExistentPartitionException, TException {
		logger.debug("Search request");

		IndexPartition partition;
		try {
			partition = this.getIndexPartition(shardId, partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(
					Lists.newArrayList(partitionId));
		}

		try {
			
			if(this.simulationDelay > 0){
				try {
					logger.debug("simulating delay: " + this.simulationDelay );
					Thread.sleep(this.simulationDelay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			return partition.search(partitionId, query, limit, offset);
		} catch (IOException e) {
			throw new TException("Could not search the index partition "
					+ partitionId, e);
		} catch (PartitionNotReadyException e) {
			throw new TException("Partition not ready: " + partitionId, e);
		}
	}

	@Override
	public boolean containsPartition(int shardId, int partitionId)
			throws TException {
		logger.debug("containsPartition request");

		return partitionMap.containsKey(new PartitionIdentifier(shardId,
				partitionId));

	}

	protected void replicate(int shardId, int partitionId) throws IOException,
			TException {

		IndexPartition partition = partitionMap.get(partitionId);

		if (partition == null) {
			// Partition does not exists. Create a new one and replicate
			this.createPartition(shardId, partitionId);
			return;
		}

		// The partition already exists and must be replicated from another node
		if (partition != null) {
			switch (partition.getStatus()) {
			case FAILURE:
			case RESTORE_FAILED:
				partition.clear();
				break;

			case READY:
				throw new IllegalStateException("The partition " + partitionId
						+ " already exists and is ready");
			case RESTORING:
				throw new IllegalStateException("The partition " + partitionId
						+ " is already replicating");
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
	public PartitionStatus partitionStatus(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {

		IndexPartition partition = null;

		try {
			partition = this.getIndexPartition(shardId, partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(
					Lists.newArrayList(partitionId));
		}

		PartitionStatus status = new PartitionStatus();
		status.partitionId = partitionId;
		status.status = partition.getStatus().toString();

		return status;
	}

	@Override
	public List<String> listPartitionFiles(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {

		IndexPartition partition = null;

		try {
			partition = this.getIndexPartition(shardId, partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(
					Lists.newArrayList(partitionId));
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
		logger.info(String.format("New partition status (%d, %d): %s",
				indexPartition.getShardId(), indexPartition.getPartitionId(),
				indexPartition.getStatus().toString()));
		logger.info(String.format("Partition (%d, %d) last error: %s",
				indexPartition.getShardId(), indexPartition.getPartitionId(),
				indexPartition.getLastError()));

		try {
			this.partitionResolver.updatePartitionStatus(
					indexPartition.getShardId(),
					indexPartition.getPartitionId(),
					this.getThisNodeDescriptor(), indexPartition.getStatus());
		} catch (PartitionResolverException e) {
			logger.error("Could not notify zookeeper about the status change",
					e);
		}
	}

	public GroupVersionTracker getVersionTracker() {
		return versionTracker;
	}

	public void setVersionTracker(GroupVersionTracker versionTracker) {
		this.versionTracker = versionTracker;
	}

	@Override
	public void prepareCommit(int shardId, int partitionId,
			MessageId messageId, List<Document> documents)
			throws NonExistentPartitionException, IndexException, TException {
		logger.debug("prepareCommit request");

		IndexPartition partition;
		try {
			partition = this.getIndexPartition(shardId, partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(
					Lists.newArrayList(partitionId));
		}

		try {
			partition.prepare(messageId, documents);
		} catch (IOException e) {
			logger.error("Could not prepare commit", e);
			throw new IndexException("IO error while preparing commit: "
					+ e.getMessage());
		} catch (PartitionNotReadyException e) {
			logger.error("The partition is not ready", e);
			throw new IndexException(
					String.format(
							"The partition is not ready. MessageId: %d, LastCommitId: %d, LastPreparedId: %d, Status: %s",
							messageId, partition.getLastCommittedMessageId(),
							partition.getLastPreparedMessageId(),
							partition.getStatus()));
		}
	}

	@Override
	public void commit(int shardId, int partitionId)
			throws NonExistentPartitionException, IndexException, TException {
		logger.debug("commit request");

		IndexPartition partition;
		try {
			partition = this.getIndexPartition(shardId, partitionId);
		} catch (IOException e) {
			throw new TException("Could not obtain index partition", e);
		}

		if (partition == null) {
			throw new NonExistentPartitionException(
					Lists.newArrayList(partitionId));
		}

		try {
			partition.commit();
		} catch (IOException e) {
			logger.error("Could not commit", e);
			throw new IndexException("IO error while preparing commit: "
					+ e.getMessage());
		}
	}

	@Override
	public List<PartitionStatus> listPartitions() throws TException {

		List<PartitionStatus> result = Lists.newArrayList();

		for (PartitionIdentifier partition : this.partitionMap.keySet()) {
			PartitionStatus ps = new PartitionStatus(partition.shardId,
					partition.partitionId, this.partitionMap.get(partition)
							.getStatus().toString());
			result.add(ps);
		}

		return result;
	}

	@Override
	public int totalDocuments(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setSimulationDelay(int simulateDelay) {
		this.simulationDelay = simulateDelay;		
	}

}
