package org.vanilladb.calvin.Communication;

import java.io.Serializable;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class StoredPorc implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private long txNum;

	private int clientId, rteId, pId;
	
	private Object objs;

	private SpResultSet result;

	public StoredPorc(int clienId, int rteid, int pid, Object... objs) {
		this.clientId = clienId;
		this.rteId = rteid;
		this.pId = pid;
		this.objs = objs;
	}
	
	public StoredPorc(int clienId, int pid, Object... objs) {
		this.clientId = clienId;
		this.pId = pid;
		this.objs = objs;
	}
	
	public void setTxNum(long tx) {
		this.txNum = tx;
	}
	
	public long getTxNum() {
		return txNum;
	}
	
	public int getPid() {
		return pId;
	}

	public SpResultSet getResultSet() {
		return result;
	}

	public Object getPars() {
		return objs;
	}
	
	public int getClientId() {
		return clientId;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public int getRteId() {
		return rteId;
	}

	public void setRteId(int rteId) {
		this.rteId = rteId;
	}
}
