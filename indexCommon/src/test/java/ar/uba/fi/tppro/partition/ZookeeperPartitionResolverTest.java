package ar.uba.fi.tppro.partition;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;

import ar.uba.fi.tppro.core.index.IndexNodeDescriptor;
import ar.uba.fi.tppro.core.index.IndexPartitionStatus;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;

public class ZookeeperPartitionResolverTest {

	@Test
	public void testZkPartition() throws Exception {
		
		TestingServer server = new TestingServer();

		try {
			CuratorFramework client1 = CuratorFrameworkFactory.newClient(
					server.getConnectString(), new RetryOneTime(1));
			
			CuratorFramework client2 = CuratorFrameworkFactory.newClient(
					server.getConnectString(), new RetryOneTime(1));
			
			CuratorFramework client3 = CuratorFrameworkFactory.newClient(
					server.getConnectString(), new RetryOneTime(1));
			
			client1.start();
			client2.start();
			client3.start();
			
			ZookeeperPartitionResolver partitionResolver1 = new ZookeeperPartitionResolver(client1);
			ZookeeperPartitionResolver partitionResolver2 = new ZookeeperPartitionResolver(client2);
			ZookeeperPartitionResolver partitionResolver3 = new ZookeeperPartitionResolver(client3);

			
			IndexNodeDescriptor nd1 = mock(IndexNodeDescriptor.class);
			IndexNodeDescriptor nd2 = mock(IndexNodeDescriptor.class);
			IndexNodeDescriptor nd3 = mock(IndexNodeDescriptor.class);
			
			when(nd1.getHost()).thenReturn("host-nd1");
			when(nd2.getHost()).thenReturn("host-nd2");
			when(nd3.getHost()).thenReturn("host-nd3");
			when(nd1.getPort()).thenReturn(1001);
			when(nd2.getPort()).thenReturn(1002);
			when(nd3.getPort()).thenReturn(1003);
			
			partitionResolver1.registerPartition(1, 1, nd1, IndexPartitionStatus.READY);
			partitionResolver1.registerPartition(1, 1, nd2, IndexPartitionStatus.READY);
			partitionResolver2.registerPartition(1, 1, nd3, IndexPartitionStatus.READY);
			partitionResolver1.registerPartition(2, 1, nd2, IndexPartitionStatus.READY);
			partitionResolver2.registerPartition(3, 2, nd3, IndexPartitionStatus.READY);
			partitionResolver1.registerPartition(3, 2, nd1, IndexPartitionStatus.READY);
			partitionResolver2.registerPartition(2, 2, nd3, IndexPartitionStatus.READY);
			partitionResolver1.registerPartition(2, 2, nd1, IndexPartitionStatus.READY);
			partitionResolver1.registerPartition(2, 2, nd2, IndexPartitionStatus.READY);
			partitionResolver1.registerPartition(1, 3, nd2, IndexPartitionStatus.READY);
			partitionResolver2.registerPartition(1, 2, nd3, IndexPartitionStatus.READY);


			Multimap<Integer, IndexNodeDescriptor> partitionList1 = partitionResolver3.resolve(1);
			Multimap<Integer, IndexNodeDescriptor> partitionList2 = partitionResolver3.resolve(2);
			Multimap<Integer, IndexNodeDescriptor> partitionList3 = partitionResolver3.resolve(3);
			
			Set<Integer> group1Parts = Sets.newHashSet(1, 2, 3);
			Set<Integer> group2Parts = Sets.newHashSet(1, 2);
			Set<Integer> group3Parts = Sets.newHashSet(2);

			for(Integer partId : partitionList1.keySet()){
				assertTrue(group1Parts.contains(partId));
				group1Parts.remove(partId);
			}
			
			for(Integer partId : partitionList2.keySet()){
				assertTrue(group2Parts.contains(partId));
				group2Parts.remove(partId);
			}
			
			for(Integer partId : partitionList3.keySet()){
				assertTrue(group3Parts.contains(partId));
				group3Parts.remove(partId);
			}
			
			assertTrue(group1Parts.size() == 0);
			assertTrue(group2Parts.size() == 0);
			assertTrue(group3Parts.size() == 0);

			client2.close();
			
			Multimap<Integer, IndexNodeDescriptor> partitionClosed = partitionResolver3.resolve(1);
			
			assertEquals(2, partitionClosed.keySet().size());

			
		} finally{
			server.close();
		}
	}

}
