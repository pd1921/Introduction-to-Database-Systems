package org.vanilladb.calvin.schedule.Calvin;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.ManuallyAbortException;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.calvin.schedule.calvinStoredProcedure;
import org.vanilladb.calvin.server.CalvinDb;
import org.vanilladb.calvin.sql.RecordKey;
import org.vanilladb.calvin.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.vanilladb.calvin.storage.tx.recovery.CalvinRecoveryMgr;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper> implements calvinStoredProcedure {

	private static Logger logger = Logger.getLogger(CalvinStoredProcedure.class.getName());
	
	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;

	// Record keys
	private List<RecordKey> readKeys = new ArrayList<RecordKey>();
	private List<RecordKey> writeKeys = new ArrayList<RecordKey>();
	private RecordKey[] readKeysForLock, writeKeysForLock;

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}


	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	protected abstract void prepareKeys();

	/**
	 * Perform the transaction logic and record the result of the transaction.
	 */
	protected abstract void performTransactionLogic();


	/**********************
	 * Implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// create transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		this.tx = CalvinDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		
		// prepare keys
		prepareKeys();
	}

	public void requestConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();

		readKeysForLock = readKeys.toArray(new RecordKey[0]);
		writeKeysForLock = writeKeys.toArray(new RecordKey[0]);

		ccMgr.prepareSp(readKeysForLock, writeKeysForLock);
	}

	public final RecordKey[] getReadSet() {
		return readKeysForLock;
	}

	public final RecordKey[] getWriteSet() {
		return writeKeysForLock;
	}

	public SpResultSet execute() {

		try {
			// Get conservative locks it has asked before
			getConservativeLocks();

			// Execute transaction
			performTransactionLogic();

			// The transaction finishes normally
			tx.commit();

		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}

		return paramHelper.createResultSet();
	}

	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}

	@Override
	public boolean isMaster() {
		return true;
	}
	
	@Override
	public boolean isParticipant() {
		return true;
	}

	protected void addReadKey(RecordKey readKey) {
		readKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		writeKeys.add(writeKey);
	}
	
	protected H getParamHelper() {
		return paramHelper;
	}
	
	protected Transaction getTransaction() {
		return tx;
	}
	
	protected void abort() {
		throw new ManuallyAbortException();
	}
	
	protected void abort(String message) {
		throw new ManuallyAbortException(message);
	}
	
	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		ccMgr.executeSp(readKeysForLock, writeKeysForLock);
	}
}