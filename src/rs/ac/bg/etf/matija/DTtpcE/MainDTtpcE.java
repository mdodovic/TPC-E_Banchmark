package rs.ac.bg.etf.matija.DTtpcE;

import java.io.PrintWriter;
import java.sql.Connection;

public class MainDTtpcE {

	
	private Connection connection;

	public MainDTtpcE(Connection connection) {
		this.connection = connection;
	}

	
	public void startCustomerPositionTransaction(
			long cust_id, String tax_id, int get_history, int acct_idx) {
	}

	
	public void startMarketFeedTransaction(
			double[] price_quote, String status_submitted, 
			String[] symbol, long[] trade_qty, 
			String type_limit_buy, String type_limit_sell, String type_stop_loss) {
		
		
	}
	
	
	public void startTradeResult(long acct_id, 
			String symbol, int hs_qty, int trade_qty, double se_amount, int trNum) {
		
	}

	
	public void startTransactionMixture(
			String pathToData, PrintWriter timestamp, PrintWriter difference) {
		
		
		
		
	}


	
}
