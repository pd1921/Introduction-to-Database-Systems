package org.vanilladb.calvin.stroage.metadata;

import org.vanilladb.calvin.sql.RecordKey;
import org.vanilladb.calvin.util.CalvinProperties;

public abstract class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;

	static {
		NUM_PARTITIONS = CalvinProperties.getLoader().getPropertyAsInteger(
				PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
	}
	
	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 * @return the partition id
	 */
	public abstract int getPartition(RecordKey key);
}