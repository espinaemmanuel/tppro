package ar.uba.fi.tppro.core.index.versionTracker;

public class StaleVersionException extends Exception {

	public StaleVersionException(int count) {
		super("Different version in the server: " + count );
	}

}
