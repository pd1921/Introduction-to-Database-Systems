package org.vanilladb.bench.server.procedure.as2;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.server.param.as2.UPdatePriceProcParamHelper;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class UpdatePriceProc extends StoredProcedure<UPdatePriceProcParamHelper> {
	public UpdatePriceProc() {
		super(new UPdatePriceProcParamHelper());
	}

	@Override
	protected void executeSql() {
		UPdatePriceProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
		
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			// read
			String name;
			double price;
			
			int iid = paramHelper.getReadItemId(idx);
			Plan p = VanillaDb.newPlanner().createQueryPlan(
					"SELECT i_name, i_price FROM item WHERE i_id = " + iid, tx);
			Scan s = p.open();
			s.beforeFirst();
			if (s.next()) {
				name = (String) s.getVal("i_name").asJavaVal();
				price = (Double) s.getVal("i_price").asJavaVal();
				//System.out.println(name + ": " + price);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);
			s.close();
			// update
			if(price + paramHelper.getPriceRaise(idx) > As2BenchConstants.MAX_PRICE) 
				price = As2BenchConstants.MIN_PRICE;
			else price += paramHelper.getPriceRaise(idx);
			
			int p1 = VanillaDb.newPlanner().executeUpdate(
					"UPDATE item SET i_price = " + String.valueOf(price) + " WHERE i_id = " + iid, tx);
			//Scan s1 = p1.open();
			
			// debug
			/*Plan p5 = VanillaDb.newPlanner().createQueryPlan(
					"SELECT i_name, i_price FROM item WHERE i_id = " + iid, tx);
			Scan s5 = p5.open();
			s5.beforeFirst();
			if (s5.next()) {
				name = (String) s5.getVal("i_name").asJavaVal();
				price = (Double) s5.getVal("i_price").asJavaVal();
				System.out.println(name + ": " + price);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);
			s5.close();*/
			// debug
			
			
			
			//s1.beforeFirst();
			if (p1>0) {
				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(price, idx);
			} else
				throw new RuntimeException("Cloud not update item record with i_id = " + iid);
			
		}
	}
}
