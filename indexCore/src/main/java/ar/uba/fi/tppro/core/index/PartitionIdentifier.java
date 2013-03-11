package ar.uba.fi.tppro.core.index;

public class PartitionIdentifier {

	private int shardId;
	private int partitionId;

	public PartitionIdentifier(int shardId, int partitionId) {
		this.shardId = shardId;
		this.partitionId = partitionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + partitionId;
		result = prime * result + shardId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PartitionIdentifier other = (PartitionIdentifier) obj;
		if (partitionId != other.partitionId)
			return false;
		if (shardId != other.shardId)
			return false;
		return true;
	}

}
