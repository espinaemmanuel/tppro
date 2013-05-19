package ar.uba.fi.tppro.core.service;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;

import ar.uba.fi.tppro.core.index.ClusterManager.ClusterManager;
import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.partition.StaticSocketPartitionResolver;

public abstract class IndexCoreTest {
	
	protected StaticSocketPartitionResolver partitionResolver;

	protected GroupVersionTracker versionTracker;

	protected LockManager lockManager;
		
	protected ClusterManager clusterManager;
	
	final Logger logger = LoggerFactory.getLogger(IndexCoreTest.class);

	
	@Rule
	public TemporaryFolder testFolder = new TemporaryFolder();
	
	protected IndexServer createIndexServer(CuratorFramework client, int port) throws IOException{
		File dataDir = testFolder.newFolder();
		logger.debug("Temp dir: " + dataDir);
		
		if(dataDir.list().length > 0)
			fail("temp directory not empty");
		
		IndexServerConfig config1 = new IndexServerConfig();
		config1.listenPort = port;
		config1.dataDir = dataDir;
		
		IndexServer core = new IndexServer(config1);
		core.setPartitionResolver(partitionResolver);
		core.setVersionTracker(versionTracker);
		core.setLockManager(lockManager);
		core.setCurator(client);
		core.setClusterManager(clusterManager);
		
		return core;
	}

}
