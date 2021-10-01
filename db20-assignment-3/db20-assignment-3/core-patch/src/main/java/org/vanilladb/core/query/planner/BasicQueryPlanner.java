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
package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;


import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

import org.vanilladb.core.query.algebra.ExplainPlan;

/**
 * The simplest, most naive query planner possible.
 */
public class BasicQueryPlanner implements QueryPlanner {

	/**
	 * Creates a query plan as follows. It first takes the product of all tables
	 * and views; it then selects on the predicate; and finally it projects on
	 * the field list.
	 */
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// TODO: save info of record, block and table's name
		/*List<Long> rec_table = new ArrayList<Long>();
		List<Long> blk_table = new ArrayList<Long>();
		List<String> name_table = new ArrayList<String>();
		String predString = data.pred().toString();
		long rec_product=0, rec_select=0, rec_sort=0, rec_groupby=0, rec_project=0;
		long blk_product=0, blk_select=0, blk_sort=0, blk_groupby=0, blk_project=0;*/
		
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		int i=0;
		for (String tblname : data.tables()) {
			// TODO: save tblname
			//name_table.add(tblname);
			
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else
				plans.add(new TablePlan(tblname, tx));
			
			Plan temp = plans.get(i);
			i++;
			//blk_table.add(temp.blocksAccessed());
			//rec_table.add(temp.recordsOutput());
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);
		//rec_product = p.recordsOutput();
		//blk_product = p.blocksAccessed();
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		//rec_select = p.recordsOutput();
		//blk_select = p.blocksAccessed();
		// Step 4: Add a group-by plan if specified
		if (data.groupFields() != null) {
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
		}
		//rec_groupby = p.recordsOutput();
		//blk_groupby = p.blocksAccessed();
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		//rec_project = p.recordsOutput();
		//blk_project = p.blocksAccessed();
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null)
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
		//rec_sort = p.recordsOutput();
		//blk_sort = p.blocksAccessed();
		// TODO: explain
		//System.out.println(data.is_explainFn());  // OK, is true
		 
		// Step 7: Add a Explain Plan if specified
		if (data.is_explainFn())
			p = new ExplainPlan(p);
		
		return p;
	}
}
