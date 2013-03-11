namespace java ar.uba.fi.tppro.core.service.thrift
namespace php tppro


struct Document {
	1: map<string, string> fields,
}

struct Hit {
	1: double score,
	2: Document doc,
}

struct QueryResult {
	1: i32 totalHits,
	2: string parsedQuery,
	3: list<Hit> hits,
}

exception ParseException {
  1: string msg,
}

exception NonExistentPartitionException {
  1: list<i32> partitionId,
}

exception PartitionAlreadyExistsException {
  1: i32 partitionId,
}

exception ParalellSearchException {
	1: string msg,
}

exception ParalellIndexException {
	1: string msg,
}

exception IndexException {
	1: string msg,
}

exception ReplicationException {
	1: i32 code,
	2: string msg
}

struct Error {
	1: i32 code,
	2: string desc,
}

struct ParalellSearchResult {
	1: QueryResult qr,
	2: list<Error> errors,
}

struct IndexResult {
	1: list<Error> errors,
}

struct PartitionStatus {
	1: i32 partitionId,
	2: string status,
}

service IndexBroker{
	ParalellSearchResult search(1: i32 shardId, 2: string query, 3: i32 limit, 4: i32 offset) throws (1:ParalellSearchException searex, 2:NonExistentPartitionException partex),
	IndexResult deleteByQuery(1: i32 shardId, 2: string query),
	IndexResult index(1: i32 shardId, 2: list<Document> documents) throws (1:ParalellIndexException parex, 2:NonExistentPartitionException noex),
}
 
service IndexNode{
	QueryResult search(1: i32 shardId, 2: i32 partitionId, 3: string query, 4: i32 limit, 5: i32 offset) throws (1:ParseException parsex, 2:NonExistentPartitionException partex),
	void deleteByQuery(1: i32 shardId, 2: i32 partitionId, 3: string query),
	void prepareCommit(1: i32 shardId, 2: i32 partitionId, 3: i32 messageId, 4: list<Document> documents) throws (1: NonExistentPartitionException nonEx, 2: IndexException indexEx),
	void commit(1: i32 shardId, 2: i32 partitionId) throws (1: NonExistentPartitionException nonEx, 2: IndexException indexEx),
	void createPartition(1: i32 shardId, 2: i32 partitionId) throws (1: PartitionAlreadyExistsException partex),
	void removePartition(1: i32 shardId, 2: i32 partitionId) throws (1: NonExistentPartitionException partex),
	bool containsPartition(1: i32 shardId, 2: i32 partitionId),
	PartitionStatus partitionStatus(1: i32 shardId, 2: i32 partitionId) throws (1: NonExistentPartitionException e),
	list<string> listPartitionFiles(1: i32 shardId, 2: i32 partitionId) throws (1: NonExistentPartitionException e),
}
	