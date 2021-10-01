package org.vanilladb.calvin.schedule.Calvin;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.calvin.Communication.StoredPorc;
import org.vanilladb.calvin.schedule.Scheduler;
import org.vanilladb.calvin.schedule.calvinStoredProcedure;
import org.vanilladb.calvin.schedule.calvinStoredProcedureFactory;
import org.vanilladb.calvin.server.task.calvinStoredProcedureTask;
import org.vanilladb.calvin.storage.tx.recovery.CalvinRecoveryMgr;
import org.vanilladb.calvin.util.CalvinProperties;

public abstract class CalvinScheduler extends Task implements Scheduler {

	private static final Class<?> FACTORY_CLASS;
	private BlockingQueue<StoredPorc> spcQueue = new LinkedBlockingQueue<StoredPorc>();
	
	static {
		FACTORY_CLASS = CalvinProperties.getLoader().getPropertyAsClass(
						CalvinScheduler.class.getName() + ".FACTORY_CLASS", null, 
						CalvinStoredProcedureFactory.class);
	}
	
	private calvinStoredProcedureFactory factory;
	
	public CalvinScheduler() {
		try {
			factory = (CalvinStoredProcedureFactory) FACTORY_CLASS.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true) {
			try {
				// retrieve stored procedure call
				StoredPorc call = spcQueue.take();

				// create store procedure and prepare
				calvinStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());

				// log request
				if (!sp.isReadOnly())
					calvinRecoveryMgr.logRequest(call);

				// create a new task for multi-thread
				CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
						call.getClientId(), call.getRteId(), call.getTxNum(),
						sp);

				// perform conservative locking
				spt.lockConservatively();

				// hand over to a thread to run the task
				VanillaDb.taskMgr().runTask(spt);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void schedule(StoredPorc... calls) {
		// TODO Auto-generated method stub
		
		try {
			for(int i=0; i<calls.length; i++) {
				spcQueue.put(calls[i]);
			} 
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
}
