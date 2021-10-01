package org.vanilladb.core.query.algebra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Bucket;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.sql.Type;

public class ExplainPlan implements Plan{
	/**
	 * Returns a histogram that, for each field, approximates the value
	 * distribution of products from the specified histograms.
	 * 
	 * @param hist1
	 *            the left-hand-side histogram
	 * @param hist2
	 *            the right-hand-side histogram
	 * @return a histogram that, for each field, approximates the value
	 *         distribution of the products
	 */
	public static Histogram productHistogram(Histogram hist1) {
		return hist1;
	}

	private Plan p1;
	private Schema schema;
	private Histogram hist;
	
	/*private List<Long> rec_table;
	private List<Long> blk_table;
	private List<String> name_table;
	private String predString;
	private long rec_product=0, rec_select=0, rec_sort=0, rec_groupby=0, rec_project=0;
	private long blk_product=0, blk_select=0, blk_sort=0, blk_groupby=0, blk_project=0;
    */
	
	/**
	 * Creates a new product node in the query tree, having the two specified
	 * subqueries.
	 * 
	 * @param p1
	 *            the left-hand subquery
	 * @param p2
	 *            the right-hand subquery
	 */
	public ExplainPlan(Plan p1) {
		this.p1 = p1;
	}

	/**
	 * Creates a product scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s1 = p1.open(); // s1 is explain's scan
		String explain_query_plan = "";
		String a = explained(explain_query_plan, 0, false);
		//System.out.println("1: " + a);
		return new ExplainScan(s1, a, schema());
	}
	
	
	@Override
	public String explained(String explain_query_plan, int whiteSpace, boolean is_product) {
		/*String ans = "";
		for (int i=0; i<explain_query_plan.size(); i++) {
			String whiteSpace = "";
			String temp = "";
			temp = explain_query_plan.get(explain_query_plan.size() - 1 - i);
			for (int j=0; j<i*4; j++) whiteSpace += " ";
			
			ans += whiteSpace;
			ans += temp;
			ans += '\n';
		}
		System.out.println(ans);*/
		//System.out.println(explain_query_plan);
		return p1.explained(explain_query_plan, whiteSpace, false);
	}

	/**
	 * Estimates the number of block accesses in the product. The formula is:
	 * 
	 * <pre>
	 * B(product(p1, p2)) = B(p1) + R(p1) * B(p2)
	 * </pre>
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p1.blocksAccessed();
	}

	/**
	 * Returns the schema of the product, which is the union of the schemas of
	 * the underlying queries.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		Schema schema = new Schema();
		schema.addField("query-plan", Type.VARCHAR(500));
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return hist;
	}

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 * 
	 * @see Plan#recordsOutput()
	 */
	@Override
	public long recordsOutput() {
		return 1;
	}

	
}
