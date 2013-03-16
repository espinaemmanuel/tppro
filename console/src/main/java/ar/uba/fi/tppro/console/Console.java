package ar.uba.fi.tppro.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class Console {
	
	private static Map<String, Command> commands = Maps.newHashMap();
	final static Logger logger = LoggerFactory.getLogger(Console.class);


	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
		
		Reflections reflections = new Reflections("ar.uba.fi.tppro.console.commands");
	    Set<Class<? extends Command>> subTypes = reflections.getSubTypesOf(Command.class);
	    
	    for(Class<? extends Command> command : subTypes){
	    	Command newCommand = command.newInstance();
	    	commands.put(newCommand.getName(), newCommand);
	    }
	    
	    Context context = new Context();
		
		String curLine = ""; // Line read from standard in

		System.out.println("Enter a line of text (type 'quit' to exit): ");
		InputStreamReader converter = new InputStreamReader(System.in);
		BufferedReader in = new BufferedReader(converter);

		while (!(curLine.equals("quit"))) {
			curLine = in.readLine();
			
			if(curLine.equals("help")){
				for(String commandName : commands.keySet()){
					System.out.println(commandName);
				}
				continue;
			}

			if (!(curLine.equals("quit"))) {
				String[] parts = curLine.split(" ");
				
				if(commands.containsKey(parts[0])){
					try {
						commands.get(parts[0]).execute(parts, context);
					} catch (TException e) {
						logger.error("Remote exception on command: " + e.getMessage());
					} catch (Exception e) {
						logger.error("Exception on command", e);
					}
				} else {
					System.out.println("Command not found");
				}
				
			}
		}

	}
	
}
