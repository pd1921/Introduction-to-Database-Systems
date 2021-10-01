package org.vanilladb.calvin.schedule.Calvin;

import org.vanilladb.calvin.schedule.calvinStoredProcedureFactory;

public interface CalvinStoredProcedureFactory extends calvinStoredProcedureFactory {
	
	@Override
	CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum);
	
}