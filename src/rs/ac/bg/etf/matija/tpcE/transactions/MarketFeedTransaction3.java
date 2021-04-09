package rs.ac.bg.etf.matija.tpcE.transactions;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MarketFeedTransaction3 {

	private double[] price_quote;
	//private String status_submitted;
	private String[] symbol;
	private long[] trade_qty;
	//private String type_limit_buy;
	//private String type_limit_sell;
	//private String type_stop_loss;
	
	private Connection databaseConnection;


	public MarketFeedTransaction3(Connection databaseConnection, double[] price_quote, String status_submitted, String[] symbol, long[] trade_qty,
			String type_limit_buy, String type_limit_sell, String type_stop_loss) {
		
		this.price_quote = price_quote;
		//this.status_submitted = status_submitted;
		this.symbol = symbol;
		this.trade_qty = trade_qty;
		//this.type_limit_buy = type_limit_buy;
		//this.type_limit_sell = type_limit_sell;
		//this.type_stop_loss = type_stop_loss;
		this.databaseConnection = databaseConnection;
	}


	public void invokeMarketFeedFrame1() {
		long millis = System.currentTimeMillis();
		Date now_dts = new Date(millis); // current date
		/*
		TradeRequestBuffer[]
		long req_price_quote S_PRICE_T
		long req_trade_id TRADE_T
		long req_trade_qty S_QTY_T
		String req_trade_type 
		int rows_sent;
		*/
		int rows_updated = 0;
		
		
		for(int i = 0; i < price_quote.length/*Constraints.max_feed_len*/; i++) {
			// must be as a signle transaction with rollback mechanism

			String updateLastTrade = "update LAST_TRADE "
					+ "set LT_PRICE = ?, LT_VOL = LT_VOL + ?, LT_DTS = ?  "
					+ "WHERE LT_S_SYMB = ?";

			try (PreparedStatement stmt = databaseConnection.prepareStatement(updateLastTrade)){

				databaseConnection.setAutoCommit(false);
				
				stmt.setDouble(1, price_quote[i]);
				stmt.setLong(2, trade_qty[i]);			
				stmt.setDate(3, now_dts);
				stmt.setString(4, symbol[i]);			

				int row_count = stmt.executeUpdate();				
				rows_updated = rows_updated + row_count;
				//System.out.println("MF: " + symbol[i]);
				databaseConnection.commit();
				
			} catch (SQLException e) {
				e.printStackTrace();
				try {
					databaseConnection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}finally {
				try {
					databaseConnection.setAutoCommit(true);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
		
		
	}

	
}
