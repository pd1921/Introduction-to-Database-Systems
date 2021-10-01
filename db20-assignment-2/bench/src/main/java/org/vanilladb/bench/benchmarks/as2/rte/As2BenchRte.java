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
package org.vanilladb.bench.benchmarks.as2.rte;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.as2.As2BenchTxnType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.util.RandomValueGenerator;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;

public class As2BenchRte extends RemoteTerminalEmulator<As2BenchTxnType> {
	
	private As2BenchTxExecutor executor;

	public As2BenchRte(SutConnection conn, StatisticMgr statMgr) {
		super(conn, statMgr);
		//System.out.println('1');
		//executor = new As2BenchTxExecutor(new UpdatePriceParamGen());
	}
	
	protected As2BenchTxnType getNextTxType() {  // change return type to control the action
		RandomValueGenerator rvg = new RandomValueGenerator();
		double rate = rvg.fixedDecimalNumber(1, 0.0,  10.0);
		//System.out.println(rn);
		if(rate >= As2BenchConstants.READ_WRITE_TX_RATE) {
			//System.out.println(As2BenchConstants.READ_WRITE_TX_RATE);
			//executor = new As2BenchTxExecutor(new UpdatePriceParamGen());
			return As2BenchTxnType.UPDATE_ITEM;
		}
		else {
			//System.out.println('2');
			//executor = new As2BenchTxExecutor(new As2ReadItemParamGen());
			return As2BenchTxnType.READ_ITEM;
		}

	}
	
	protected As2BenchTxExecutor getTxExeutor(As2BenchTxnType type) {
		if(type == As2BenchTxnType.UPDATE_ITEM) executor = new As2BenchTxExecutor(new UpdatePriceParamGen());
		else executor = new As2BenchTxExecutor(new As2ReadItemParamGen());
		return executor;
	}
}
