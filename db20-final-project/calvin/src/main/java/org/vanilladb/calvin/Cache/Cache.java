package org.vanilladb.calvin.Cache;

import java.util.HashMap;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.storage.tx.Transaction;

public class Cache {
	private HashMap<CacheRecord, Constant> RecExisting = new HashMap<CacheRecord, Constant>();
    
    Cache() {  
    }

    public void cacheRemoteRec(String tlbName, String fldName, Transaction tx, Constant val, Predicate pred) {
    	CacheRecord Rec = new CacheRecord(tlbName, fldName, pred);
    	if(!Rec.isExisted(RecExisting))
    		RecExisting.put(Rec, val);
    }
    
    public Constant retrieveRec(String tlbName, String fldName, Transaction tx, Predicate pred) {
    	TablePlan p = new TablePlan(tlbName, tx);
    	TableScan s = (TableScan) p.open();
    	Constant temp;
    	s.beforeFirst();
    	while (s.next()) {
    		if (pred.isSatisfied(s)) {
    			temp = s.getVal(fldName);
    			s.close();
    			return temp;
    		}
    	}
    	return null;
    }
    
    public void modifyRec(String tlbName, String fldName, Transaction tx, Constant val, Predicate pred) {
    	TablePlan p = new TablePlan(tlbName, tx);
    	TableScan s = (TableScan) p.open();
    	s.beforeFirst();
    	while (s.next()) {
    		if (pred.isSatisfied(s))
    			s.setVal(fldName, val);
    	}
    	s.close();
    }
    
    public void insertRec(String tlbName, Transaction tx) {
    	TablePlan p = new TablePlan(tlbName, tx);
    	TableScan s = (TableScan) p.open();
    	s.insert();
    	s.close();
    }
    
    public void deleteRec(String tlbName, Transaction tx, Predicate pred) {
    	TablePlan p = new TablePlan(tlbName, tx);
    	TableScan s = (TableScan) p.open();
    	s.beforeFirst();
    	while (s.next()) {
    		if (pred.isSatisfied(s))
    			s.delete();
    	}
    	s.close();
    }
}
