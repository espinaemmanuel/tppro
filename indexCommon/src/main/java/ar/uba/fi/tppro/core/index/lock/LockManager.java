package ar.uba.fi.tppro.core.index.lock;


public abstract class LockManager {
	
	public enum LockType{
		ADD,
		DELETE
	}
	
	public abstract IndexLock aquire(LockType type, int timeout) throws LockAquireTimeoutException;

	public abstract void release(IndexLock indexLock);

}
