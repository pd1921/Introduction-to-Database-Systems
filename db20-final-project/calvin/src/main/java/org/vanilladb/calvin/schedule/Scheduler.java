package org.vanilladb.calvin.schedule;

import org.vanilladb.calvin.Communication.StoredPorc;

public interface Scheduler {
	
	void schedule(StoredPorc... calls);
}