namespace java ar.uba.fi.tppro.core.service.thrift

struct Document {
	1: map<string, string> fields,
}

struct QueryResult {
	1: double score,
	2: Document doc,
}

exception ParseException {
  1: string msg,
}

 
service IndexNode{
	list<QueryResult> search(1: i32 partitionId, 2: string query, 3: i32 limit, 4: i32 offset) throws (1:ParseException pe),
	void deleteByQuery(1: i32 partitionId, 2: string query),
	void index(1: i32 partitionId, 2: list<Document> documents),
	void createPartition(1: i32 partitionId),
	void removePartition(1: i32 partitionId),
}
	