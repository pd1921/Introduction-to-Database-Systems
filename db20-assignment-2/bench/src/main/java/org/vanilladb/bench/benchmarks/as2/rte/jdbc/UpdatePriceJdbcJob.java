package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.vanilladb.bench.util.RandomValueGenerator;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;

public class UpdatePriceJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdatePriceJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		// Parse parameters
		int readCount = (Integer) pars[0];
		int[] itemIds = new int[readCount];
		for (int i = 0; i < readCount; i++)
			itemIds[i] = (Integer) pars[i + 1];
		
		// I do, generate 10 random price
		RandomValueGenerator rvg = new RandomValueGenerator();
		double[] price_raise = new double[10];
		for (int i = 0; i < 10; i++)
			price_raise[i] = rvg.fixedDecimalNumber(1, 0.0, 5.0);
			
		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
		
		// Execute logic
		try {
			//System.out.println("update");
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			int update_return = 0;
			double price;
			for (int i = 0; i < 10; i++) {
				// i do, get price from item.i_price
				String sql = "SELECT i_price, i_name FROM item WHERE i_id = " + itemIds[i];
				rs = statement.executeQuery(sql);
				rs.beforeFirst();
				if (rs.next()) {
					price = rs.getDouble("i_price");
					//System.out.println(itemIds[i] + ": " + price);
					//outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
				} else
					throw new RuntimeException("cannot find the record with i_id = " + itemIds[i]);
				//rs.close();
				
				// i do, check if the price exceeds As2BenchConstants.MAX_PRICE
				if(price + price_raise[i] > As2BenchConstants.MAX_PRICE) 
					price = As2BenchConstants.MIN_PRICE;
				else price += price_raise[i];
				
				// i do, UpdatePrice
				String sql2 = "UPDATE item SET i_price = " + String.valueOf(price) + " WHERE i_id = " + itemIds[i];
				update_return = statement.executeUpdate(sql2);
				if (update_return > 0) {
					outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
					//System.out.println(itemIds[i] + ": " + price);
				} else
					throw new RuntimeException("cannot update the record with i_id = " + itemIds[i]);
				rs.close();
			}
			conn.commit();
			
			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");
			
			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}
