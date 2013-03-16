package ar.uba.fi.tppro.console;

public interface Command {
	
	public String getName();
	
	public void execute(String argv[], Context context) throws Exception;

}
