package rs.ac.bg.etf.matija.tpcE;

import java.sql.Connection;


public class MainTPCE {

	public static final String[] tableNames = new String[] {
			"HOLDING",
			"TRADE_HISTORY",
			"SETTLEMENT",
			"CASH_TRANSACTION",
			"HOLDING_HISTORY",
			"TRADE_REQUEST",
			"TRADE",
			"ACCOUNT_PERMISSION",
			"HOLDING_SUMMARY",
			"DAILY_MARKET",
			"WATCH_ITEM",
			"CUSTOMER_TAXRATE",
			"FINANCIAL",
			"NEWS_XREF",
			"LAST_TRADE",
			"CUSTOMER_ACCOUNT",
			"COMPANY_COMPETITOR",
			"SECURITY",
			"COMPANY",
			"COMMISSION_RATE",
			"EXCHANGE",
			"WATCH_LIST",
			"CUSTOMER",
			"ADDRESS",
			"ZIP_CODE",
			"TAXRATE",
			"BROKER",
			"STATUS_TYPE",
			"INDUSTRY",
			"SECTOR",
			"NEWS_ITEM",
			"CHARGE",
			"TRADE_TYPE"		
		};

	private int status;

	private Connection connection;
	

	

	public MainTPCE(Connection connection) {
		this.connection = connection;
	}

	public void startCustomerPositionTransaction(long cust_id, String tax_id, int get_history, int acct_idx) {

		CustomerPositionTransaction2 cpT2 = new CustomerPositionTransaction2(this.connection,cust_id, tax_id,get_history,acct_idx);
		// Transaction:
		// Frame 1:
		cpT2.invokeCustomerPositionFrame1();
		
		if (cpT2.acc_len < 1 || cpT2.acc_len > Constraints.max_acct_len_rows) {
			this.status = -211;
			if(status < 0) {
				System.err.println("Unexpected error!");
			}
		}
		
		// Frame 2:
		if (get_history == 1) {

			cpT2.invokeCustomerPositionFrame2();
			
			if (cpT2.hist_len < 10 || cpT2.acc_len > Constraints.max_hist_len_rows) {
				this.status = -211;
				if(status < 0) {
					System.err.println("Unexpected error!");
				}
			}
			
		}
		
	}
	
	public void startMarketFeedTransaction(double[] price_quote, String status_submitted, String[] symbol, long[] trade_qty, String type_limit_buy, String type_limit_sell, String type_stop_loss) {
		

		
		MarketFeedTransaction3 T3 = new MarketFeedTransaction3(this.connection,
				price_quote,
				status_submitted,
				symbol,
				trade_qty,
				type_limit_buy,
				type_limit_sell,
				type_stop_loss
				);
		// Transaction:
		// Frame 1:
		T3.invokeMarketFeedFrame1();

	}
	
	public void startTradeResult(long acct_id, double se_amount) {
		
		TradeResultTransaction8 T8 = new TradeResultTransaction8(this.connection,
				acct_id ,se_amount);
		
		// Transaction:

		// Frame 6:
		T8.invokeTradeResultFrame6();

	}
	
	public void startTransactionMixture() {
		long currentTime = System.currentTimeMillis();
		
		// data!
/*		long cust_id = 0; // 4300001645L;
		String tax_id = "616NU4395SX314";
		int get_history = 0;
		int acct_idx = 3;
*/
		long cust_id = 0;
		String tax_id = "916PV7558WK395";
		int get_history = 0;
		int acct_idx = -1;
		
		startCustomerPositionTransaction(cust_id, tax_id, get_history, acct_idx);
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);

		
		// data!
		double[] price_quote = {-30.0, -35.0, -18.9};
		String status_submitted = "";
		String[] symbol = {"AA", "AACB", "AACBPRA" };
		long[] trade_qty = {-2,-3,-4};
//		String type_limit_buy = "";
//		String type_limit_sell = "";
//		String type_stop_loss = "";
//		int unique_symbols;
		
		startMarketFeedTransaction(price_quote, status_submitted, symbol, trade_qty, "", "", "");
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);
		// data!
		// for frame 6:!?
		
		long acct_id = 43000016443l;
		double se_amount = -100000.0;
		
		startTradeResult(acct_id, se_amount);
		
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);
	
		startCustomerPositionTransaction(cust_id, tax_id, get_history, acct_idx);
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);

		startCustomerPositionTransaction(cust_id, tax_id, get_history, acct_idx);
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);

		startCustomerPositionTransaction(cust_id, tax_id, get_history, acct_idx);
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);
		
		startCustomerPositionTransaction(cust_id, tax_id, get_history, acct_idx);
		
		currentTime = System.currentTimeMillis() - currentTime;
		System.out.println(currentTime);
	}
	


}
