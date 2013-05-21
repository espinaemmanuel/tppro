package ar.uba.fi.tppro.console.commands;

import ar.uba.fi.tppro.console.Command;
import ar.uba.fi.tppro.console.Context;
import ar.uba.fi.tppro.core.index.versionTracker.ZkShardVersionTracker;

public class SetVersion implements Command {

	public String getName() {
		return "setVersion";
	}

	public void execute(String[] argv, Context context) throws Exception {
		int groupId = Integer.parseInt(argv[1]);
		long version = Long.parseLong(argv[2]);
		
		ZkShardVersionTracker versionTracker = new ZkShardVersionTracker(context.curator);
		versionTracker.setShardVersion(groupId, version);
		
		System.out.println(String.format("New version of group %d: %d", groupId, version));
	}
}
