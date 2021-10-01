package org.vanilladb.calvin.schedule;

import org.vanilladb.calvin.sql.RecordKey;

public interface calvinStoredProcedure {
	
	RecordKey[] getReadSet();
	
	RecordKey[] getWriteSet();
	
	boolean isReadOnly();
	
	boolean isMaster();
	
	boolean isParticipant();

	void prepare(Object pars);

}