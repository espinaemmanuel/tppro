package ar.uba.fi.tppro.core.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

import ar.uba.fi.tppro.core.index.IndexPartition.Status;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.lock.LockManager.LockType;
import ar.uba.fi.tppro.core.service.IndexCoreHandler;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;

public class PartitionReplicator extends Thread {

	final Logger logger = LoggerFactory.getLogger(IndexCoreHandler.class);

	private Integer partitionId;
	private LockManager lockManager;
	private IndexPartition indexPartition;
	private PartitionStatusObserver statusObserver;

	private PartitionResolver partitionResolver;

	private static int LOCK_TIMEOUT = 10000;

	@Override
	public void run() {

		IndexLock lock = null;

		try {
			lock = lockManager.aquire(LockType.ADD, LOCK_TIMEOUT);
			Multimap<Integer, IndexNodeDescriptor> partitions = partitionResolver
					.resolve(Lists.newArrayList(partitionId));
			Collection<IndexNodeDescriptor> nodeDescriptors = partitions
					.get(partitionId);

			for (int i = 0; i < nodeDescriptors.size(); i++) {
				IndexNodeDescriptor nodeDescriptor = Iterables.get(
						nodeDescriptors, 0);
				IndexNode.Iface nodeClient = null;

				List<String> files = null;

				try {
					nodeClient = nodeDescriptor.getClient();
					files = nodeClient.listPartitionFiles(partitionId);

				} catch (IndexNodeDescriptorException e) {
					logger.error("Could not get node descriptor client", e);
					continue;
				} catch (NonExistentPartitionException e) {
					logger.error("The partition was not found in the node", e);
					continue;
				} catch (TException e) {
					logger.error("Error conecting to remote partition", e);
					continue;
				}

				PartitionHttpClient httpClient = nodeDescriptor.getHttpClient(partitionId);

				File tempDir = Files.createTempDir();

				try {
					for (String fileName : files) {
						
						if(fileName.equals("write.lock")) continue;
						
						InputStream remoteIndexFile = httpClient
								.getFile(fileName);
						File outputFile = new File(tempDir, fileName);
						FileOutputStream outputStream = new FileOutputStream(outputFile);
						ByteStreams.copy(remoteIndexFile, outputStream);	
					}
				} catch (PartitionHttpClientException e) {
					logger.error("Could not retrieve the remote index file", e);
					continue;
				} catch (FileNotFoundException e) {
					logger.error("Could not open output stream", e);
					continue;
				} catch (IOException e) {
					logger.error("Could not read remote file", e);
					continue;
				}
				
				if(indexPartition.getDataPath().exists()){
					for(File indexFile : indexPartition.getDataPath().listFiles()){
						indexFile.delete();
					}
				}
				
				Files.move(tempDir, indexPartition.getDataPath());
				indexPartition.setStatus(Status.READY);
				statusObserver.notifyStatusChange(indexPartition);
				
				return;
			}
			
			indexPartition.setStatus(Status.REPLICATION_FAILED);
			indexPartition
					.setError("Could not retrieve the partition from any replica");
			statusObserver.notifyStatusChange(indexPartition);

		} catch (LockAquireTimeoutException e) {
			indexPartition.setStatus(Status.REPLICATION_FAILED);
			statusObserver.notifyStatusChange(indexPartition);
			return;
		} catch (IOException e) {
			logger.error("Error moving index directory", e);
			
			indexPartition.setStatus(Status.REPLICATION_FAILED);
			statusObserver.notifyStatusChange(indexPartition);
			return;
		} finally {
			if (lock != null) {
				lock.release();
			}
		}

	}

	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	public void setPartitionResolver(PartitionResolver partitionResolver) {
		this.partitionResolver = partitionResolver;
	}

	public void setLockManager(LockManager lockManager) {
		this.lockManager = lockManager;
	}

	public void setPartition(IndexPartition indexPartition) {
		this.indexPartition = indexPartition;
	}
	
	public PartitionStatusObserver getStatusObserver() {
		return statusObserver;
	}

	public void setStatusObserver(PartitionStatusObserver statusObserver) {
		this.statusObserver = statusObserver;
	}

	public Integer getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(Integer partitionId) {
		this.partitionId = partitionId;
	}

	public IndexPartition getIndexPartition() {
		return indexPartition;
	}

	public void setIndexPartition(IndexPartition indexPartition) {
		this.indexPartition = indexPartition;
	}

	public LockManager getLockManager() {
		return lockManager;
	}

	public PartitionResolver getPartitionResolver() {
		return partitionResolver;
	}

}
