package ar.uba.fi.tppro.core.index.lock;

import ar.uba.fi.tppro.core.index.lock.LockManager.LockType;


public class IndexLock {
	
	private LockType type;
	private LockManager manager;
	
	public IndexLock(LockType type, LockManager manager) {
		super();
		this.type = type;
		this.manager = manager;
	}
	
	public LockType getType() {
		return type;
	}

	public void setType(LockType type) {
		this.type = type;
	}

	public void release(){
		manager.release(this);
	}

}
