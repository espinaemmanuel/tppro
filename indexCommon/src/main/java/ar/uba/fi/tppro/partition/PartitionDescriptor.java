package ar.uba.fi.tppro.partition;

import ar.uba.fi.tppro.core.index.IndexPartitionStatus;

public class PartitionDescriptor {
	
	public int group;
	public int partition;
	public String url;
	public IndexPartitionStatus status;
	
	public PartitionDescriptor(int group, int partition, String url,
			IndexPartitionStatus status) {
		super();
		this.group = group;
		this.partition = partition;
		this.url = url;
		this.status = status;
	}
	
}
