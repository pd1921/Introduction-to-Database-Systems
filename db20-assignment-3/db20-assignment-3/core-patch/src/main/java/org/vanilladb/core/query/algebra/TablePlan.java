/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.query.algebra;

import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableNotFoundException;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.metadata.statistics.TableStatInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class corresponding to a table.
 */
public class TablePlan implements Plan {
	private Transaction tx;
	private TableInfo ti;
	private TableStatInfo si;
	private String tblName;

	/**
	 * Creates a leaf node in the query tree corresponding to the specified
	 * table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the calling transaction
	 */
	public TablePlan(String tblName, Transaction tx) {
		this.tblName = tblName;
		this.tx = tx;
		ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		if (ti == null)
			throw new TableNotFoundException("table '" + tblName
					+ "' is not defined in catalog.");
		si = VanillaDb.statMgr().getTableStatInfo(ti, tx);
	}

	/**
	 * Creates a table scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		return new TableScan(ti, tx);
	}

	// my method
	@Override
	public String explained(String explain_query_plan, int whiteSpace, boolean is_productplan) {
		String temp_string = "";
		for(int i=0; i<whiteSpace*4; i++) temp_string += " ";

		temp_string += "->TablePlan on (";
		temp_string += this.tblName;
		temp_string += ") (#blks=";
		temp_string += this.blocksAccessed();
		temp_string += ", #recs=";
		temp_string += this.recordsOutput();
		temp_string += ")";
		temp_string += "\n";
		
		explain_query_plan += temp_string;
		
		return explain_query_plan;
	}

	/**
	 * Estimates the number of block accesses for the table, which is obtainable
	 * from the statistics manager.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return si.blocksAccessed();
	}

	/**
	 * Determines the schema of the table, which is obtainable from the catalog
	 * manager.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return ti.schema();
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return si.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	
}
