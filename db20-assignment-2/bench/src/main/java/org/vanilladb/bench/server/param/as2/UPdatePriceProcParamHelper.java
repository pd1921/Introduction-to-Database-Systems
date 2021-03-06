package org.vanilladb.bench.server.param.as2;

import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.bench.util.RandomValueGenerator;

public class UPdatePriceProcParamHelper extends StoredProcedureParamHelper{

	// Parameters
	private int readCount;
	private int[] readItemId;

	// Results
	private String[] itemName;
	private double[] itemPrice;
	
	// random price_raise
	private double[] price_raise;
	
	public double getPriceRaise(int idx) {
		return price_raise[idx];
	}

	public int getReadCount() {
		return readCount;
	}

	public int getReadItemId(int index) {
		return readItemId[index];
	}

	public void setItemName(String s, int idx) {
		itemName[idx] = s;
	}

	public void setItemPrice(double d, int idx) {
		itemPrice[idx] = d;
	}

	@Override
	public void prepareParameters(Object... pars) {

		// Show the contents of paramters
		// System.out.println("Params: " + Arrays.toString(pars));

		int indexCnt = 0;

		readCount = (Integer) pars[indexCnt++];
		readItemId = new int[readCount];
		itemName = new String[readCount];
		itemPrice = new double[readCount];
		price_raise = new double[readCount];
		
		// random number
		RandomValueGenerator rvg = new RandomValueGenerator();
		for (int i = 0; i < 10; i++)
			price_raise[i] = rvg.fixedDecimalNumber(1, 0.0, 5.0);

		for (int i = 0; i < readCount; i++)
			readItemId[i] = (Integer) pars[indexCnt++];
	}

	

	@Override
	public Schema getResultSetSchema() {
		Schema sch = new Schema();
		Type intType = Type.INTEGER;
		Type itemPriceType = Type.DOUBLE;
		Type itemNameType = Type.VARCHAR(24);
		sch.addField("rc", intType);
		int l = itemName.length;
		for (int i = 0; i < l; i++) {
			sch.addField("i_name_" + i, itemNameType);
			sch.addField("i_price_" + i, itemPriceType);
		}
		return sch;
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		SpResultRecord rec = new SpResultRecord();
		rec.setVal("rc", new IntegerConstant(itemName.length));
		for (int i = 0; i < itemName.length; i++) {
			rec.setVal("i_name_" + i, new VarcharConstant(itemName[i], Type.VARCHAR(24)));
			rec.setVal("i_price_" + i, new DoubleConstant(itemPrice[i]));
		}
		return rec;
	}

}
