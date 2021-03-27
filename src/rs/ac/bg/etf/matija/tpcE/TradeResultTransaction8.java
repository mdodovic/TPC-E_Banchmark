package rs.ac.bg.etf.matija.tpcE;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TradeResultTransaction8 {
	
	private long acct_id;
	private double se_amount;
	
	private Connection databaseConnection;

	public TradeResultTransaction8(Connection connection, long acct_id, double se_amount) {
		this.acct_id = acct_id;
		this.se_amount = se_amount;
		this.databaseConnection = connection;
		
	}

	public void invokeTradeResultFrame6() {
		
		String updateLastTrade = "update CUSTOMER_ACCOUNT "
				+ "set CA_BAL = CA_BAL + ? "
				+ "WHERE CA_ID = ?";

		try (PreparedStatement stmt = databaseConnection.prepareStatement(updateLastTrade)){

			databaseConnection.setAutoCommit(false);
			
			stmt.setDouble(1, se_amount);
			stmt.setLong(2, acct_id);			

			stmt.executeUpdate();				
			
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
