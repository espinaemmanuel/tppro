package ar.uba.fi.tppro.core.index.lock;


public class NullLockManager extends LockManager {

	@Override
	public void release(IndexLock indexLock) {

	}

	@Override
	public IndexLock aquire(int partitionId, int timeout)
			throws LockAquireTimeoutException {
		return new IndexLock(this);
	}

}
