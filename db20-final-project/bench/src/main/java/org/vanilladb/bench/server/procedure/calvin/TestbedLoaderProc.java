package org.vanilladb.bench.server.procedure.calvin;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.server.param.micro.TestbedLoaderParamHelper;
import org.vanilladb.bench.util.DoublePlainPrinter;
import org.vanilladb.bench.util.RandomValueGenerator;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.calvin.schedule.Calvin.CalvinStoredProcedure;
import org.vanilladb.calvin.server.CalvinDb;
import org.vanilladb.calvin.sql.RecordKey;

public class TestbedLoaderProc extends CalvinStoredProcedure<TestbedLoaderParamHelper> {
	private static Logger logger = Logger.getLogger(TestbedLoaderProc.class
			.getName());

	private RandomValueGenerator rg = new RandomValueGenerator();

	public TestbedLoaderProc(long txNum) {
		super(txNum, new TestbedLoaderParamHelper());
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
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		RecoveryMgr.logSetVal(false);

		// Generate item records
		generateItems(1, TpccConstants.NUM_ITEMS);

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		RecoveryMgr.logSetVal(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished.");
	}

	private void generateItems(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.fine("Start populating items from i_id=" + startIId
					+ " to i_id=" + endIId);

		int iid, iimid;
		String iname, idata;
		double iprice;
		String sql;
		for (int i = startIId, count = 1; i <= endIId; i++, count++) {
			iid = i;

			// Randomly generate values
			iimid = rg.number(TpccConstants.MIN_IM, TpccConstants.MAX_IM);
			iname = rg.randomAString(TpccConstants.MIN_I_NAME,
					TpccConstants.MAX_I_NAME);
			iprice = rg.fixedDecimalNumber(TpccConstants.MONEY_DECIMALS,
					TpccConstants.MIN_PRICE, TpccConstants.MAX_PRICE);
			idata = rg.randomAString(TpccConstants.MIN_I_DATA,
					TpccConstants.MAX_I_DATA);
			if (Math.random() < 0.1)
				idata = fillOriginal(idata);

			sql = "INSERT INTO item(i_id, i_im_id, i_name, i_price, i_data) VALUES ("
					+ iid
					+ ", "
					+ iimid
					+ ", '"
					+ iname
					+ "', "
					+ DoublePlainPrinter.toPlainString(iprice)
					+ ", '"
					+ idata
					+ "' )";

			HashMap<String, Constant> map = new HashMap<String, Constant>();
			map.put("i_id", new IntegerConstant(iid));
			map.put("i_im_id", new IntegerConstant(iimid));
			map.put("i_name", new VarcharConstant(iname));
			map.put("i_price", new DoubleConstant(iprice));
			map.put("i_data", new VarcharConstant(idata));
			
			int result = -1;
			RecordKey key = new RecordKey("item", map);
			int nodeId = CalvinDb.partitionMetaMgr().getPartition(key);
			if (nodeId == CalvinDb.serverId()) {
				result = VanillaDb.newPlanner().executeUpdate(sql, tx);
			
			if (result <= 0)
				throw new RuntimeException();
			
			if (count % 10000 == 0 && logger.isLoggable(Level.FINE))
				logger.fine(count + " items have been populated");}
		}

		if (logger.isLoggable(Level.FINE))
			logger.info("Populating items completed.");
	}

	private String fillOriginal(String data) {
		int originalLength = TpccConstants.ORIGINAL_STRING.length();
		int position = rg.number(0, data.length() - originalLength);
		String out = data.substring(0, position)
				+ TpccConstants.ORIGINAL_STRING
				+ data.substring(position + originalLength);
		return out;
	}

	@Override
	public void prepare(Object pars) {
		// TODO Auto-generated method stub
		
	}
}
