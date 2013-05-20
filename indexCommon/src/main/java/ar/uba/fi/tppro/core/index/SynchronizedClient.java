package ar.uba.fi.tppro.core.index;

import java.util.List;

import org.apache.thrift.TException;

import ar.uba.fi.tppro.core.service.thrift.Document;
import ar.uba.fi.tppro.core.service.thrift.IndexException;
import ar.uba.fi.tppro.core.service.thrift.IndexNode;
import ar.uba.fi.tppro.core.service.thrift.MessageId;
import ar.uba.fi.tppro.core.service.thrift.NonExistentPartitionException;
import ar.uba.fi.tppro.core.service.thrift.ParseException;
import ar.uba.fi.tppro.core.service.thrift.PartitionAlreadyExistsException;
import ar.uba.fi.tppro.core.service.thrift.PartitionStatus;
import ar.uba.fi.tppro.core.service.thrift.QueryResult;

public class SynchronizedClient implements IndexNode.Iface {
	
	
	public IndexNode.Iface wrapped;
	
	public SynchronizedClient(IndexNode.Iface wrapped){
		this.wrapped = wrapped;
	}

	@Override
	synchronized public QueryResult search(int shardId, int partitionId, String query,
			int limit, int offset) throws ParseException,
			NonExistentPartitionException, TException {
		return wrapped.search(shardId, partitionId, query, limit, offset);
	}

	@Override
	synchronized public void deleteByQuery(int shardId, int partitionId, String query)
			throws TException {
		wrapped.deleteByQuery(shardId, partitionId, query);		
	}

	@Override
	synchronized public void prepareCommit(int shardId, int partitionId,
			MessageId messageId, List<Document> documents)
			throws NonExistentPartitionException, IndexException, TException {
		wrapped.prepareCommit(shardId, partitionId, messageId, documents);		
	}

	@Override
	synchronized public void commit(int shardId, int partitionId)
			throws NonExistentPartitionException, IndexException, TException {
		wrapped.commit(shardId, partitionId);		
	}

	@Override
	synchronized public void createPartition(int shardId, int partitionId)
			throws PartitionAlreadyExistsException, TException {
		wrapped.createPartition(shardId, partitionId);		
	}

	@Override
	synchronized public void removePartition(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {
		wrapped.removePartition(shardId, partitionId);		
	}

	@Override
	synchronized public boolean containsPartition(int shardId, int partitionId)
			throws TException {
		return wrapped.containsPartition(shardId, partitionId);
	}

	@Override
	synchronized public List<PartitionStatus> listPartitions() throws TException {
		return wrapped.listPartitions();
	}

	@Override
	synchronized public int totalDocuments(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {
		return wrapped.totalDocuments(shardId, partitionId);
	}

	@Override
	synchronized public PartitionStatus partitionStatus(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {
		return wrapped.partitionStatus(shardId, partitionId);
	}

	@Override
	synchronized public List<String> listPartitionFiles(int shardId, int partitionId)
			throws NonExistentPartitionException, TException {
		return wrapped.listPartitionFiles(shardId, partitionId);
	}

}
