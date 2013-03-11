package ar.uba.fi.tppro.partition;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;
import ar.uba.fi.tppro.core.index.RemoteIndexNodeDescriptor;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.netflix.curator.framework.CuratorFramework;

public class ZookeeperPartitionResolver implements PartitionResolver {

	private CuratorFramework client;
	private String BASE_PATH = "/replicas";

	public ZookeeperPartitionResolver(CuratorFramework curatorClient) {
		this.client = curatorClient;
	}

	@Override
	public void registerPartition(int shardId, int partitionId,
			IndexNodeDescriptor descriptor, IndexPartitionStatus status)
			throws PartitionResolverException {

		String path = pathForReplica(shardId, partitionId,
				descriptor.getHost(), descriptor.getPort());
		byte[] statusBytes = status.toString().getBytes();

		final int MAX_TRIES = 2;
		boolean isDone = false;

		try {
			for (int i = 0; !isDone && (i < MAX_TRIES); ++i) {
				try {
					client.create().creatingParentsIfNeeded()
							.withMode(CreateMode.EPHEMERAL)
							.forPath(path, statusBytes);
					isDone = true;
				} catch (KeeperException.NodeExistsException e) {
					client.delete().forPath(path);
				}
			}
		} catch (Exception e) {
			throw new PartitionResolverException("curator exception:", e);
		}
	}

	@Override
	public Multimap<Integer, IndexNodeDescriptor> resolve(int shardId)
			throws PartitionResolverException {

		Multimap<Integer, IndexNodeDescriptor> partitionMap = LinkedListMultimap
				.create();

		String path = pathForShard(shardId);
		List<String> replicas;

		try {

			try {
				replicas = client.getChildren().forPath(path);
			} catch (KeeperException.NoNodeException e) {
				replicas = Lists.newArrayList();
			}

			for (String replica : replicas) {
				String[] replicaParts = replica.split("_");
				int partitionId = Integer.parseInt(replicaParts[0]);
				String host = replicaParts[1];
				int port = Integer.parseInt(replicaParts[2]);

				byte[] bytes = client.getData().forPath(path + "/" + replica);
				IndexPartitionStatus status = IndexPartitionStatus
						.valueOf(new String(bytes));

				if (status == IndexPartitionStatus.READY) {
					IndexNodeDescriptor descriptor = new RemoteIndexNodeDescriptor(
							host, port);
					partitionMap.put(partitionId, descriptor);
				}
			}
		} catch (Exception e) {
			throw new PartitionResolverException("curator exception", e);
		}

		return partitionMap;
	}

	private String pathForShard(int shardId) {
		return String.format("%s/%d", BASE_PATH, shardId);
	}

	protected String pathForReplica(int shardId, int partitionId, String host,
			int port) {
		return String.format("%s/%d/%d_%s_%d", BASE_PATH, shardId, partitionId,
				host, port);
	}

}
