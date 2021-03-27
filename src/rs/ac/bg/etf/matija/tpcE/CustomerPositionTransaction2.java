package rs.ac.bg.etf.matija.tpcE;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;



public class CustomerPositionTransaction2 {
	
	// CARows 
	private long rowidCustomerAccountRows = 1; // identity(1,1): start value is 1 and it is incremented by 1 every time
	private Map<Long, CustomerAccountRow> customerAccountRowData = 
			new HashMap<Long, CustomerPositionTransaction2.CustomerAccountRow>();
	
	// CustomerRows
	private long rowidCustomerRows = 1; // identity(1,1)
	private Map<Long, CustomerRow> customerRowData = 
			new HashMap<Long, CustomerPositionTransaction2.CustomerRow>();
	
	// TradeRows
	private long rowidTradeRows = 1; // identity(1,1)
	private Map<Long, TradeRow> tradeRowData = 
			new HashMap<Long, CustomerPositionTransaction2.TradeRow>();
	
	
	
	private long cust_id;
	private String tax_id;
	private int get_history;
	private long acct_idx;
	
	private Connection databaseConnection;
	private long acct_id;
	
	public int acc_len;
	public int hist_len;
	
	public CustomerPositionTransaction2(Connection dbConn, long cust_id, String tax_id, int get_history, long acct_idx) {
		this.databaseConnection = dbConn;
		this.cust_id = cust_id;
		this.tax_id = tax_id;
		this.get_history = get_history;
		this.acct_idx = acct_idx;

	}
	
	public void invokeCustomerPositionFrame1() {
		/*
		 *  Parameters of this frame:

 			long cust_id;
			String tax_id;
			int get_history;
			long acct_idx;

		 */
		if(cust_id == 0) {
			
			String getIdFromTaxIdQuery = "select C_ID from CUSTOMER where C_TAX_ID = ?";
			try (PreparedStatement stmt = databaseConnection.prepareStatement(getIdFromTaxIdQuery)){
				stmt.setString(1, tax_id);
				try(ResultSet rs = stmt.executeQuery()){
				
					if(rs.next() == true) {
						cust_id = rs.getLong(1);
					} /*else {
						throw new Exception("customer with this C_TAX_ID does not exist");
					}*/
				}
			} catch(SQLException e) { 
				e.printStackTrace(); 
			}
			
		}
		// Now we only operate with cust_id 
		// System.out.println(cust_id);
		
		// Fetch all customer data:
		String getCustomerInfo = "SELECT C_ST_ID,C_L_NAME,C_F_NAME,C_M_NAME,C_GNDR,C_TIER,C_DOB,C_AD_ID,C_CTRY_1,C_AREA_1,\r\n" + 
				"	C_LOCAL_1,C_EXT_1,C_CTRY_2,C_AREA_2,C_LOCAL_2,C_EXT_2,C_CTRY_3,C_AREA_3,C_LOCAL_3,\r\n" + 
				"	C_EXT_3,C_EMAIL_1,C_EMAIL_2\r\n" + 
				"FROM CUSTOMER\r\n" + 
				"WHERE C_ID = ?;";
		
		try (PreparedStatement stmt = databaseConnection.prepareStatement(getCustomerInfo)){
			stmt.setLong(1, cust_id);
			try(ResultSet rs = stmt.executeQuery()){
								
				fillDataToCustomerRows(rs);
				
				for (Long key : this.customerRowData.keySet()) {
				    System.out.println(customerRowData.get(key));
				}
			}
		} catch(SQLException e) { 
			e.printStackTrace(); 
		}

		// Fetch account info:
		String getCustomerAccountInfo = "SELECT TOP 10 CA_ID,CA_BAL,((sum(HS_QTY * LT_PRICE))) as RES_SUM\r\n" + 
				"FROM\r\n" + 
				"	CUSTOMER_ACCOUNT left outer join\r\n" + 
				"	HOLDING_SUMMARY on HS_CA_ID = CA_ID,\r\n" + 
				"	LAST_TRADE\r\n" + 
				"where\r\n" + 
				"	CA_C_ID = ? and\r\n" + 
				"	LT_S_SYMB = HS_S_SYMB\r\n" + 
				"	group by CA_ID, CA_BAL\r\n" + 
				"	order by 3 asc;";
		
		try (PreparedStatement stmt = databaseConnection.prepareStatement(getCustomerAccountInfo)){
			//stmt.setInt(1, Constraints.max_acct_len_rows);
			stmt.setLong(1, cust_id);
			try(ResultSet rs = stmt.executeQuery()){
			
				
				fillDataToCustomerAccountRows(rs);

				for (Long key : this.customerAccountRowData.keySet()) {
				    System.out.println(key + ". " + customerAccountRowData.get(key));
				}

			}
		} catch(SQLException e) { 
			e.printStackTrace(); 
		}

		
		
		
		if (get_history == 1) {
			
			this.acct_id = customerAccountRowData.get(acct_idx + 1).CA_ID;
			
		}
		
		acc_len = customerAccountRowData.size();
	}

	public void invokeCustomerPositionFrame2() {
		/*
		 *  Parameters of this frame:

 			long acct_id;
		 */
		
		String getTradeInfo = "	SELECT TOP 30 ID, T_S_SYMB,T_QTY,ST_NAME,TH_DTS\r\n" + 
				"	FROM (\r\n" + 
				"		SELECT TOP 10 T_ID as ID, T_S_SYMB, T_QTY\r\n" + 
				"		FROM TRADE\r\n" + 
				"		WHERE T_CA_ID = ? \r\n" + 
				"		ORDER by T_DTS desc\r\n" + 
				"	) as T, TRADE_HISTORY, STATUS_TYPE\r\n" + 
				"	where\r\n" + 
				"		TH_T_ID = ID and ST_ID = TH_ST_ID\r\n" + 
				"	order by TH_DTS desc";
		
		try (PreparedStatement stmt = databaseConnection.prepareStatement(getTradeInfo)){
			stmt.setLong(1, acct_id);
			try(ResultSet rs = stmt.executeQuery()){
				
				fillDataToTradeRows(rs);
				
				for (Long key : this.tradeRowData.keySet()) {
				    System.out.println(key + ". " + tradeRowData.get(key));
				}

			}
		} catch(SQLException e) { 
			e.printStackTrace(); 
		}
		
		hist_len = tradeRowData.size();
		
	}
	
	
	private void fillDataToTradeRows(ResultSet rs) throws SQLException {
		
		while (rs.next()) {
			TradeRow tr = new TradeRow();
			
			tr.ST_NAME = rs.getString("ST_NAME");
			tr.T_ID = rs.getLong("ID");
			tr.T_QTY = rs.getInt("T_QTY");
			tr.T_S_SYMB = rs.getString("T_S_SYMB");
			tr.TH_DTS = rs.getTimestamp("TH_DTS");
			
			tradeRowData.put(rowidTradeRows, tr);
			rowidTradeRows += 1;
		}

		
	}
	
	private void fillDataToCustomerAccountRows(ResultSet rs) throws SQLException {
		while (rs.next()) {
			CustomerAccountRow car = new CustomerAccountRow();
			car.CA_BAL = rs.getLong("CA_BAL");
			car.CA_ID = rs.getLong("CA_ID");
			car.result = rs.getLong("RES_SUM");

			customerAccountRowData.put(rowidCustomerAccountRows, car);
			rowidCustomerAccountRows += 1;
		}

		
	}
	
	private void fillDataToCustomerRows(ResultSet rs) throws SQLException {
		//ResultSetMetaData rsmd = rs.getMetaData(); // contains number of columns
		while (rs.next()) {
			CustomerRow cr = new CustomerRow();
			cr.C_AD_ID = rs.getLong("C_AD_ID");
			cr.C_AREA_1 = rs.getString("C_AREA_1");
			cr.C_AREA_2 = rs.getString("C_AREA_2");
			cr.C_AREA_3 = rs.getString("C_AREA_3");
			cr.C_CTRY_1 = rs.getString("C_CTRY_1");
			cr.C_CTRY_2 = rs.getString("C_CTRY_2");
			cr.C_CTRY_3 = rs.getString("C_CTRY_3");
			cr.C_CTRY_1 = rs.getString("C_CTRY_1");
			cr.C_CTRY_2 = rs.getString("C_CTRY_2");
			cr.C_CTRY_3 = rs.getString("C_CTRY_3");
			cr.C_DOB = rs.getDate("C_DOB");
			cr.C_EMAIL_1 = rs.getString("C_EMAIL_1");
			cr.C_EMAIL_2 = rs.getString("C_EMAIL_2");
			cr.C_EXT_1 = rs.getString("C_EXT_1");
			cr.C_EXT_2 = rs.getString("C_EXT_2");
			cr.C_EXT_3 = rs.getString("C_EXT_3");
			cr.C_F_NAME = rs.getString("C_F_NAME");
			cr.C_GNDR = rs.getString("C_GNDR");
			cr.C_L_NAME = rs.getString("C_L_NAME");
			cr.C_LOCAL_1 = rs.getString("C_LOCAL_1");
			cr.C_LOCAL_2 = rs.getString("C_LOCAL_2");
			cr.C_LOCAL_3 = rs.getString("C_LOCAL_3");
			cr.C_M_NAME = rs.getString("C_M_NAME");
			cr.C_ST_ID = rs.getString("C_ST_ID");
			cr.C_TIER = rs.getInt("C_TIER");
			
			customerRowData.put(rowidCustomerRows, cr);
			rowidCustomerRows += 1;
		}
		
	}
	
	class CustomerRow {

		private String C_ST_ID;
		private String C_L_NAME;
		private String C_F_NAME;
		private String C_M_NAME;
		private String C_GNDR;
		private int C_TIER;
		private Date C_DOB;
		private long C_AD_ID;
	
		private String C_CTRY_1;
		private String C_AREA_1;
		private String C_LOCAL_1;
		private String C_EXT_1;
	
		private String C_CTRY_2;
		private String C_AREA_2;
		private String C_LOCAL_2;
		private String C_EXT_2;
	
		private String C_CTRY_3;
		private String C_AREA_3;
		private String C_LOCAL_3;
		private String C_EXT_3;
	
		private String C_EMAIL_1;
		private String C_EMAIL_2; 
		
		@Override
		public String toString() {
			return C_ST_ID + " " + 
			C_L_NAME + " " + 
			C_F_NAME + " " + 
			C_M_NAME + " " + 
			C_GNDR + " " + 
			C_TIER + " " + 
			C_DOB + " " + 
			C_AD_ID + " " + 
		
			C_CTRY_1 + " " + 
			C_AREA_1 + " " + 
			C_LOCAL_1 + " " + 
			C_EXT_1 + " " + 
		
			C_CTRY_2 + " " + 
			C_AREA_2 + " " + 
			C_LOCAL_2 + " " + 
			C_EXT_2 + " " + 
		
			C_CTRY_3 + " " + 
			C_AREA_3 + " " + 
			C_LOCAL_3 + " " + 
			C_EXT_3 + " " + 
		
			C_EMAIL_1 + " " + 
			C_EMAIL_2;

		}
	}

	class CustomerAccountRow {
		private long CA_ID;		
		private double CA_BAL;
		private double result; // decimal(20,2)
		
		@Override
		public String toString() {
			return 	CA_ID + " " + 
					CA_BAL + " " + 
					result;
		}
	}
	
	class TradeRow {

		private long T_ID;
		private String T_S_SYMB;
		private int T_QTY;
		private String ST_NAME;
		private Timestamp TH_DTS;
	
		@Override
		public String toString() {
			return 	T_ID + " " + 
					T_S_SYMB + " " + 
					T_QTY + " " + 
					ST_NAME + " " +
					TH_DTS;
		}
		
	}

}
