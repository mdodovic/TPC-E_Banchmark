package rs.ac.bg.etf.matija.tpcE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MainTPCE {

	public static final String USER = "sa";
	public static final String PASSWORD = "matija";
	
	Connection connection;
	
	public Connection getConnection() {
		return connection;
	}
	
	public void connectToMSSQL() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		
		
			String x = "jdbc:sqlserver://localhost:1433;databaseName=tpcE;";
	
			connection = DriverManager.getConnection(x, USER, PASSWORD);
	
			
		
			System.out.println("Connected to MS SQL Server");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public void fetchTable() {
		String s = "	SELECT * FROM tpcE.INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'proba' AND TABLE_SCHEMA = 'dbo'";
		
		Statement stm;
		try {
			stm = connection.createStatement();
			ResultSet rs = stm.executeQuery(s);
		
		while(rs.next()) {
			System.out.println(rs.getString(3));
			
		}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	public void fetchData() {
		String s = "SELECT TOP (1000) [CH_TT_ID], [CH_C_TIER], [CH_CHRG]" + 
				"  FROM [tpcE].[dbo].[proba]";
		
		Statement stm;
		try {
			stm = connection.createStatement();
			ResultSet rs = stm.executeQuery(s);
		
			while(rs.next()) {
				System.out.println(rs.getString(1) + " ; " + rs.getString(2) + " ; " + rs.getString(3));
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void addData() {
		String s = "INSERT INTO [dbo].[proba]" + 
				"           ([CH_TT_ID], [CH_C_TIER],[CH_CHRG])" + 
				"     VALUES" + 
				"           ('d', 7, 11.5)";
		
		Statement stm;
		try {
			stm = connection.createStatement();
			int rs = stm.executeUpdate(s);
		
			System.out.println(rs);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args) {

		MainTPCE database = new MainTPCE();		
		database.connectToMSSQL();

		NormalizedChema.createNormalizedDatabaseChema(database.getConnection());
		
		//database.fetchTable();
		//database.addData();
		//database.fetchData();
		
	}

}
