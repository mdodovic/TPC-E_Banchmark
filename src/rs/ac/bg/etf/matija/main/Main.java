package rs.ac.bg.etf.matija.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import rs.ac.bg.etf.matija.tpcE.MainTPCE;
import rs.ac.bg.etf.matija.tpcE.NormalizedChemaCreator;

public class Main {

	public static final String USER = "sa";
	public static final String PASSWORD = "matija";

	private Connection connection;
	
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
	
	
	public static void main(String[] args) {
		long start = System.currentTimeMillis();

		Main database = new Main();		
		database.connectToMSSQL();
		
		MainTPCE tpcEOriginalSchema = new MainTPCE(database.getConnection());
		
		System.out.println("Database creation ... finished");

		/*try {
			NormalizedChemaCreator.dropNormalizedDatabaseChema(database.getConnection());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		//NormalizedChemaCreator.createNormalizedDatabaseChema(database.getConnection());
		//$System.out.println(System.currentTimeMillis() - start);
		System.out.println("Database schema creation ... finished");
		
		//NormalizedChemaLoader.loadData(database.getConnection());
		//$System.out.println(System.currentTimeMillis() - start);
		
		NormalizedChemaCreator.createIndexes(database.getConnection());
		System.out.println("Loading data ... finished");
		
		System.out.println("Cold start ... finished after " + (System.currentTimeMillis() - start) / 1000. + " seconds");
		
		tpcEOriginalSchema.startTransactionMixture();
		

	}
}
