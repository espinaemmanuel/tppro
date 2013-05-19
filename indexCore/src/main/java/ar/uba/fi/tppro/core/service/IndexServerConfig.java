package ar.uba.fi.tppro.core.service;

import java.io.File;

import ar.uba.fi.tppro.core.index.lock.LockManager;
import ar.uba.fi.tppro.core.index.versionTracker.GroupVersionTracker;
import ar.uba.fi.tppro.partition.PartitionResolver;

public class IndexServerConfig{
	public String bindIp;
	public int listenPort;
	public File dataDir;
	public PartitionResolver partitionResolver;
	public GroupVersionTracker versionTracker;
	public LockManager lockManager;
	public String zookeeperUrl;
}