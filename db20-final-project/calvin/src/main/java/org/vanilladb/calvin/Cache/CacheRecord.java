package org.vanilladb.calvin.Cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.predicate.Predicate;

public class CacheRecord{
	private String tblName;
	private String fldName;
	private Predicate pred;
	
	public CacheRecord(String tblName, String fldName, Predicate pred) {
		this.fldName = fldName;
		this.tblName = tblName;
		this.pred = pred;
	}
	
	public boolean isExisted(HashMap<CacheRecord, Constant> RecExisting) {
		Set<CacheRecord> RecSet = RecExisting.keySet();
		for(CacheRecord rec: RecSet) {
			if(this.fldName == rec.fldName && this.tblName == rec.tblName && this.pred == rec.pred)
				return true;
        }
		return false;
	}

	
}
