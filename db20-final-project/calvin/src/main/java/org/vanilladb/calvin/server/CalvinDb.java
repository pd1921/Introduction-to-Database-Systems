package org.vanilladb.calvin.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.SampleStoredProcedureFactory;
import org.vanilladb.calvin.cache.CacheMgr;
import org.vanilladb.calvin.cache.Calvin.CalvinCacheMgr;
import org.vanilladb.calvin.Communication.ConnectionMgr;
import org.vanilladb.calvin.schedule.Scheduler;
import org.vanilladb.calvin.schedule.Calvin.CalvinStoredProcedureFactory;
import org.vanilladb.calvin.schedule.Calvin.CalvinScheduler;
import org.vanilladb.calvin.storage.log.CalvinLogMgr;
import org.vanilladb.calvin.stroage.metadata.PartitionMetaMgr;
import org.vanilladb.calvin.util.CalvinProperties;

public class CalvinDb {

	// Logger
	private static Logger logger = Logger.getLogger(CalvinDb.class.getName());

	// Managers
	private static ConnectionMgr connMgr;
	private static CalvinLogMgr logMgr;
	private static CacheMgr cacheMgr;
	private static PartitionMetaMgr parMetaMgr;
	private static Scheduler scheduler;

	// Utility classes
	private static CalvinStoredProcedureFactory spFactory;

	// connection ID
	private static int myNodeId;
	
	/**
	 * Initialization Flag
	 */
	private static boolean inited;


	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 * @param factory
	 *            the stored procedure factory for generating stored procedures
	 */
	public static void init(String dirName, int id) {

		myNodeId = id;
		
		if (inited) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("discarding duplicated init request");
			return;
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("calvinDb initializing...");

		VanillaDb.init(dirName, new SampleStoredProcedureFactory());
		
		initCacheMgr();
		initPartitionMetaMgr();
		initScheduler();
		initConnectionMgr(myNodeId);
		initCalvinLogMgr();

		// finish initialization
		inited = true;
	}

	/**
	 * Is VanillaDB initialized ?
	 * 
	 * @return true if it is initialized, otherwise false.
	 */
	public static boolean isInited() {
		return inited;
	}

	/*
	 * The following initialization methods are useful for testing the
	 * lower-level components of the system without having to initialize
	 * everything.
	 */


	public static void initCacheMgr() {
		cacheMgr = new CalvinCacheMgr();
	}

	
	public static void initScheduler() {
		scheduler = new CalvinScheduler(); 
	}

	/**
	 * Initializes the connection manager.
	 */
	public static void initConnectionMgr(int id) {
		connMgr = new ConnectionMgr(id);
	}

	/**
	 * Initializes the Calvin's log manager.
	 */
	public static void initCalvinLogMgr() {
		logMgr = new CalvinLogMgr(); 
	}

	public static void initPartitionMetaMgr() {
		parMetaMgr = new PartitionMetaMgr();
	}


	public static CacheMgr cacheMgr() {
		return cacheMgr;
	}

	public static CalvinLogMgr CalvinLogMgr() {
		return logMgr;
	}

	public static Scheduler scheduler() {
		return scheduler;
	}

	public static PartitionMetaMgr partitionMetaMgr() {
		return parMetaMgr;
	}

	public static ConnectionMgr connectionMgr() {
		return connMgr;
	}


	public static int serverId() {
		return myNodeId;
	}

}

