package org.vanilladb.bench.server.procedure.calvin;

import org.vanilladb.bench.server.param.tpcc.TpccSchemaBuilderProcParamHelper;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.calvin.schedule.Calvin.CalvinStoredProcedure;

public class SchemaBuilderProc extends CalvinStoredProcedure<TpccSchemaBuilderProcParamHelper> {
	
	public SchemaBuilderProc(long txNum) {
		super(txNum, new TpccSchemaBuilderProcParamHelper());
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}

	@Override
	protected void performTransactionLogic() {
		// Creating a table need to be executed directly 
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);
	}

	@Override
	public void prepare(Object pars) {
		// TODO Auto-generated method stub
		
	}
}
