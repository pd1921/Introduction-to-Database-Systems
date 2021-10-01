
/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.util.BenchProperties;

public class StatisticMgr {
	private static Logger logger = Logger.getLogger(StatisticMgr.class.getName());

	private static final File OUTPUT_DIR;


	static {
		String outputDirPath = BenchProperties.getLoader().getPropertyAsString(StatisticMgr.class.getName()
				+ ".OUTPUT_DIR", null);
		
		if (outputDirPath == null) {
			OUTPUT_DIR = new File(System.getProperty("user.home"), "benchmark_results");
		} else {
			OUTPUT_DIR = new File(outputDirPath);
		}

		// Create the directory if that doesn't exist
		if (!OUTPUT_DIR.exists())
			OUTPUT_DIR.mkdir();
		
		
	}

	private static class TxnStatistic {
		private BenchTransactionType mType;
		private int txnCount = 0;
		private long totalResponseTimeNs = 0;

		public TxnStatistic(BenchTransactionType txnType) {
			this.mType = txnType;
		}

		public BenchTransactionType getmType() {
			return mType;
		}

		public void addTxnResponseTime(long responseTime) {
			txnCount++;
			totalResponseTimeNs += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTimeNs;
		}
	}

	private List<TxnResultSet> resultSets = new ArrayList<TxnResultSet>();
	private List<BenchTransactionType> allTxTypes;
	private String fileNamePostfix = "";
	private long recordStartTime = -1;
	
	public StatisticMgr(Collection<BenchTransactionType> txTypes) {
		allTxTypes = new LinkedList<BenchTransactionType>(txTypes);
	}
	
	public StatisticMgr(Collection<BenchTransactionType> txTypes, String namePostfix) {
		allTxTypes = new LinkedList<BenchTransactionType>(txTypes);
		fileNamePostfix = namePostfix;
	}
	
	/**
	 * We use the time that this method is called at as the start time for recording.
	 */
	public synchronized void setRecordStartTime() {
		if (recordStartTime == -1)
			recordStartTime = System.nanoTime();
	}

	public synchronized void processTxnResult(TxnResultSet trs) {
		if (recordStartTime == -1)
			recordStartTime = trs.getTxnEndTime();
		resultSets.add(trs);
	}

	public synchronized void outputReport() {
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss"); // E.g. "20180524-200824"
			
			String fileName = formatter.format(Calendar.getInstance().getTime());
			
			/* create the Report2 file name */
			String CSVfileName = formatter.format(Calendar.getInstance().getTime());
			
			/* change name by PD */
			//String.format(CSVfileName, "-as2bench"); // "20200417-200824-as2bench"
			
			if (fileNamePostfix != null && !fileNamePostfix.isEmpty())
				fileName += "-" + fileNamePostfix; // E.g. "20200524-200824-postfix"
			
			outputDetailReport(fileName);
			
			/* create new CSV file by PD */
			outputCSVReport(fileName);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Finnish creato ing tpcc benchmark report");
	}

	private void outputDetailReport(String fileName) throws IOException {
		Map<BenchTransactionType, TxnStatistic> txnStatistics = new HashMap<BenchTransactionType, TxnStatistic>();
		Map<BenchTransactionType, Integer> abortedCounts = new HashMap<BenchTransactionType, Integer>();
		
		for (BenchTransactionType type : allTxTypes) {
			txnStatistics.put(type, new TxnStatistic(type));
			abortedCounts.put(type, 0);
		}
		
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(OUTPUT_DIR, fileName + ".txt")))) {
			// First line: total transaction count
			writer.write("# of txns (including aborted) during benchmark period: " + resultSets.size());
			writer.newLine();
			
			// Detail latency report
			for (TxnResultSet resultSet : resultSets) {
				if (resultSet.isTxnIsCommited()) {
					// Write a line: {[Tx Type]: [Latency]}
					writer.write(resultSet.getTxnType() + ": "
							+ TimeUnit.NANOSECONDS.toMillis(resultSet.getTxnResponseTime()) + " ms");
					writer.newLine();
					
					// Count transaction for each type
					TxnStatistic txnStatistic = txnStatistics.get(resultSet.getTxnType());
					txnStatistic.addTxnResponseTime(resultSet.getTxnResponseTime());
					
					
				} else {
					writer.write(resultSet.getTxnType() + ": ABORTED");
					writer.newLine();
					
					// Count transaction for each type
					Integer count = abortedCounts.get(resultSet.getTxnType());
					abortedCounts.put(resultSet.getTxnType(), count + 1);
				}
			}
			writer.newLine();
			
			// Last few lines: show the statistics for each type of transactions
			int abortedTotal = 0;
			for (Entry<BenchTransactionType, TxnStatistic> entry : txnStatistics.entrySet()) {
				TxnStatistic value = entry.getValue();
				int abortedCount = abortedCounts.get(entry.getKey());
				abortedTotal += abortedCount;
				long avgResTimeMs = 0;
				
				if (value.txnCount > 0) {
					avgResTimeMs = TimeUnit.NANOSECONDS.toMillis(
							value.getTotalResponseTime() / value.txnCount);
				}
				
				writer.write(value.getmType() + " - committed: " + value.getTxnCount() +
						", aborted: " + abortedCount + ", avg latency: " + avgResTimeMs + " ms");
				writer.newLine();
			}
			
			// Last line: Total statistics
			int finishedCount = resultSets.size() - abortedTotal;
			double avgResTimeMs = 0;
			if (finishedCount > 0) { // Avoid "Divide By Zero"
				for (TxnResultSet rs : resultSets)
					avgResTimeMs += rs.getTxnResponseTime() / finishedCount;
			}
			writer.write(String.format("TOTAL - committed: %d, aborted: %d, avg latency: %d ms", 
					finishedCount, abortedTotal, Math.round(avgResTimeMs / 1000000)));
		}
	}
	
	
	/*To create a new csvfile
	 * use end time to record the 5 second work, then count the result.*/ 
	private void outputCSVReport(String fileName) throws IOException {
		Map<BenchTransactionType, TxnStatistic> txnStatistics = new HashMap<BenchTransactionType, TxnStatistic>();
		Map<BenchTransactionType, Integer> abortedCounts = new HashMap<BenchTransactionType, Integer>();
		
		for (BenchTransactionType type : allTxTypes) {
			txnStatistics.put(type, new TxnStatistic(type));
			abortedCounts.put(type, 0);
		}
		
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(OUTPUT_DIR, fileName + ".csv")))) {
			// The first line of the report
			StringBuilder sb = new StringBuilder();
			writer.write("time(sec), throughput(txs), avg_latency(Micros), min(Micros), max(Micros), 25th_lat(Micros), median_lat(Micros), 75th_lat(Micros)");
			writer.newLine();
			
			// latency
			long avgResTimeMs = 0;
			long totalResTimeMs = 0;
			long startResTimeMs = 0;
			int commitedCount = 0;
			int TimeCount = 0;
			List<Long> latency = new ArrayList<>(); //for saving the data in the list
			
			for (TxnResultSet resultSet : resultSets) {
				if (resultSet.isTxnIsCommited()) {
					if(startResTimeMs == 0)
						startResTimeMs = TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnEndTime());
					
					if(TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnEndTime())-startResTimeMs >= 5000000 ) {
						TimeCount += 5;
						/*count the result */
						avgResTimeMs = totalResTimeMs/commitedCount;
						
						Collections.sort(latency);
						int size = latency.size();
						
						long miden = 0; //50%
						if(size%2 == 1) {
							miden = latency.get((size-1)/2);
						}else {
							miden = (long) (( latency.get(size/2-1)+ latency.get(size/2) + 0.0)/2);
						}
						
						long firstQ = 0; //25%
						if( size%4 == 0){
							firstQ = latency.get(size/4);
						}else {
							firstQ = latency.get(size/4+1);
						}
						
						long lastQ = 0; //75%
						if( size%4 == 0) {
							lastQ = latency.get(size/4*3);
						}else {
							lastQ = latency.get(size*3/4+1);
						}
						
						/*print data*/
						writer.write(TimeCount+","+commitedCount+","+avgResTimeMs+","+latency.get(0)+","+latency.get(commitedCount-1)+","+firstQ+","+miden+","+lastQ);
						writer.newLine();
						
						/* clear the map */
						latency.clear();
						latency.add(TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnResponseTime()));
						/*put the data in the map(?) */
						startResTimeMs = TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnEndTime());
						commitedCount = 1;
					}
					else {
						commitedCount++;
						/*put the data in the map*/
						long nowResTimeMs = TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnResponseTime());
						totalResTimeMs += nowResTimeMs;
						latency.add(TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnResponseTime()));
					}
					
				} else {
					// Count transaction for each type
					Integer count = abortedCounts.get(resultSet.getTxnType());
					abortedCounts.put(resultSet.getTxnType(), count + 1);
				}
			}
		}
	}
}