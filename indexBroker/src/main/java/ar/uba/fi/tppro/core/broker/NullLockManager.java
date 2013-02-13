package ar.uba.fi.tppro.core.broker;

import ar.uba.fi.tppro.core.index.lock.IndexLock;
import ar.uba.fi.tppro.core.index.lock.LockAquireTimeoutException;
import ar.uba.fi.tppro.core.index.lock.LockManager;

public class NullLockManager extends LockManager {

	@Override
	public IndexLock aquire(LockType type, int timeout)
			throws LockAquireTimeoutException {
		return new IndexLock(type, this);
	}

	@Override
	public void release(IndexLock indexLock) {

	}

}
