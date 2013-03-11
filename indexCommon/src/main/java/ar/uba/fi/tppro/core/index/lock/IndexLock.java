package ar.uba.fi.tppro.core.index.lock;

public class IndexLock {
	
	private LockManager manager;
	
	public IndexLock(LockManager manager) {
		super();
		this.manager = manager;
	}

	public void release(){
		manager.release(this);
	}

}
