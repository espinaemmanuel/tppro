package ar.uba.fi.tppro.core.broker;

public class VersionGenerator {

	public long getNextVersion(){
		return System.currentTimeMillis();
	}
}
