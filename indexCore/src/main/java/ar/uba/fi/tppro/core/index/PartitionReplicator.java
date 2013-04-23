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

import ar.uba.fi.tppro.core.index.httpClient.PartitionHttpClient;
import ar.uba.fi.tppro.core.index.httpClient.PartitionHttpClientException;
import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.partition.PartitionResolver;
import ar.uba.fi.tppro.partition.PartitionResolverException;

public class PartitionReplicator extends Thread {

	final Logger logger = LoggerFactory.getLogger(IndexPartitionsGroup.class);

	private LockManager lockManager;
	private IndexPartition indexPartition;
	private PartitionStatusObserver statusObserver;

	private PartitionResolver partitionResolver;

	private static int LOCK_TIMEOUT = 10000;

	@Override
	public void run() {

		logger.info(String.format("Starting replication of partition (%d, %d)", indexPartition.getShardId(), indexPartition.getPartitionId()));

		IndexLock lock = null;

		try {
			lock = lockManager.aquire(indexPartition.getShardId(), LOCK_TIMEOUT);
			
			Multimap<Integer, IndexNodeDescriptor> partitions = partitionResolver.resolve(indexPartition.getShardId());
			Collection<IndexNodeDescriptor> nodeDescriptors = partitions.get(indexPartition.getPartitionId());
			
			if(nodeDescriptors.size() == 0){
				logger.warn(String.format("No partition with replica of (%d,%d) in a valid state was found", this.indexPartition.getShardId(), this.indexPartition.getPartitionId()));
			}

			for (int i = 0; i < nodeDescriptors.size(); i++) {
				IndexNodeDescriptor nodeDescriptor = Iterables.get(
						nodeDescriptors, 0);
				IndexNode.Iface nodeClient = null;

				List<String> files = null;

				try {
					nodeClient = nodeDescriptor.getClient();
					files = nodeClient.listPartitionFiles(indexPartition.getShardId(), indexPartition.getPartitionId());

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

				PartitionHttpClient httpClient = nodeDescriptor.getHttpClient(indexPartition.getShardId(), indexPartition.getPartitionId());

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
				indexPartition.reload();
				
				statusObserver.notifyStatusChange(indexPartition);
				
				return;
			}
			
			indexPartition.setStatus(IndexPartitionStatus.RESTORE_FAILED);
			indexPartition
					.setLastError("Could not retrieve the partition from any replica");
			statusObserver.notifyStatusChange(indexPartition);

		} catch (LockAquireTimeoutException e) {
			indexPartition.setStatus(IndexPartitionStatus.RESTORE_FAILED);
			statusObserver.notifyStatusChange(indexPartition);
			return;
		} catch (IOException e) {
			logger.error("Error moving index directory", e);
			
			indexPartition.setStatus(IndexPartitionStatus.RESTORE_FAILED);
			statusObserver.notifyStatusChange(indexPartition);
			return;
		} catch (PartitionResolverException e) {
			logger.error("Could not resolve partitions", e);
			
			indexPartition.setStatus(IndexPartitionStatus.RESTORE_FAILED);
			statusObserver.notifyStatusChange(indexPartition);
			return;
		} finally {
			if (lock != null) {
				lock.release();
			}
		}

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
