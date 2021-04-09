package rs.ac.bg.etf.matija.tpcE.transactions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TradeResultTransaction8 {
	
	private long acct_id;

	private String symbol;
	private int hs_qty;
	private int trade_qty;

	private double se_amount;
	private Connection databaseConnection;

	public TradeResultTransaction8(Connection connection, 
			long acct_id, String symbol, int hs_qty, int trade_qty, double se_amount) {
		this.acct_id = acct_id;

		this.symbol = symbol;
		this.hs_qty = hs_qty;
		this.trade_qty = trade_qty;

		this.se_amount = se_amount;
		this.databaseConnection = connection;
	}

	public void invokeTradeResultFrame2() {
		
		String updateHoldingSummary = "update HOLDING_SUMMARY "
				+ "set HS_QTY = ? + ? "
				+ "where HS_CA_ID = ? and HS_S_SYMB = ?";
		// Index is established already: PK = (HS_CA_ID, HS_S_SYMB);
		try (PreparedStatement stmt = databaseConnection.prepareStatement(updateHoldingSummary)){

			databaseConnection.setAutoCommit(false);
			
			stmt.setInt(1, hs_qty);
			stmt.setInt(2, trade_qty);			
			stmt.setLong(3, acct_id);
			stmt.setString(4, symbol);			

			stmt.executeUpdate();				
			//System.out.println("TRF2: " + acct_id + " + " + symbol + " = " + hs_qty + " + " + trade_qty);
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

	
	public void invokeTradeResultFrame6() {
		
		String updateLastTrade = "update CUSTOMER_ACCOUNT "
				+ "set CA_BAL = CA_BAL + ? "
				+ "WHERE CA_ID = ?";
		// Index is established already: PK = (CA_ID);
		
		try (PreparedStatement stmt = databaseConnection.prepareStatement(updateLastTrade)){

			databaseConnection.setAutoCommit(false);
			
			stmt.setDouble(1, se_amount);
			stmt.setLong(2, acct_id);			

			stmt.executeUpdate();				
			//System.out.println("TRF6: " + acct_id);
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
