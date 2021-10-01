package org.vanilladb.calvin.schedule;


public interface calvinStoredProcedureFactory {
	
	calvinStoredProcedure getStoredProcedure(int pid, long txNum);
}