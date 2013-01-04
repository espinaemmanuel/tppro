namespace java ar.uba.fi.tppro.core.service.thrift

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
  1: i32 partitionId,
}

exception PartitionAlreadyExistsException {
  1: i32 partitionId,
}

 
service IndexNode{
	QueryResult search(1: i32 partitionId, 2: string query, 3: i32 limit, 4: i32 offset) throws (1:ParseException parsex, 2:NonExistentPartitionException partex),
	void deleteByQuery(1: i32 partitionId, 2: string query),
	void index(1: i32 partitionId, 2: list<Document> documents) throws (1: NonExistentPartitionException e),
	void createPartition(1: i32 partitionId) throws (1: PartitionAlreadyExistsException partex),
	void removePartition(1: i32 partitionId) throws (1: NonExistentPartitionException partex),
	bool containsPartition(1: i32 partitionId),
}
	