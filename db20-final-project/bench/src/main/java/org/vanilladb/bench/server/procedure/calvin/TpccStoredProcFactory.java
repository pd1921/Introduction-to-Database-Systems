package org.vanilladb.bench.server.procedure.calvin;

import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.calvin.schedule.Calvin.CalvinStoredProcedure;
import org.vanilladb.calvin.schedule.Calvin.CalvinStoredProcedureFactory;

public class TpccStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (TpccTransactionType.fromProcedureId(pid)) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case CHECK_DATABASE:
			sp = new CheckDatabaseProc(txNum);
			break;
		case NEW_ORDER:
			sp = new NewOrderProc(txNum);
			break;
		case PAYMENT:
			sp = new PaymentProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}

}
