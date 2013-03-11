package ar.uba.fi.tppro.core.index.lock;


public abstract class LockManager {
	
	public abstract IndexLock aquire(int shardId, int timeout) throws LockAquireTimeoutException;

	public abstract void release(IndexLock indexLock);

}
