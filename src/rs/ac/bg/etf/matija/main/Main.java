package rs.ac.bg.etf.matija.main;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import rs.ac.bg.etf.matija.dataCreation.DataT2T3T8;
import rs.ac.bg.etf.matija.tpcE.MainTPCE;
import rs.ac.bg.etf.matija.tpcE.NormalizedChemaCreator;
import rs.ac.bg.etf.matija.tpcE.NormalizedChemaLoader;

public class Main {

	public static final String USER = "sa";
	public static final String PASSWORD = "matija";

	private Connection connection;

	public static final String pathToResultNormalized = "./src/result/normalized/";
	public static final String pathToResultIndexes = "./src/result/indexes/";
	public static final String pathToResultDenormalized = "./src/result/denormalized/";
	
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
		
		try (FileWriter fw1 = new FileWriter(pathToResultNormalized + "read_only_1kcp_timestamp.txt");
			PrintWriter timestamp = new PrintWriter(fw1);
				FileWriter fw2 = new FileWriter(pathToResultNormalized + "read_only_1kcp_difference.txt");
				PrintWriter difference = new PrintWriter(fw2)){
		

	
			// Tpce Normalized schema:
			MainTPCE tpcEOriginalSchema = new MainTPCE(database.getConnection());
			
			
			/* Drop whole schematic
			try {
				$NormalizedChemaCreator.dropNormalizedDatabaseChema(database.getConnection());
			} catch (SQLException e) {
				e.printStackTrace();
			}*/
			
			NormalizedChemaCreator.createNormalizedDatabaseChema(database.getConnection());
			//$System.out.println(System.currentTimeMillis() - start);
			System.out.println("Database schema creation ... finished");
			
			NormalizedChemaLoader.loadData(database.getConnection());
			//$System.out.println(System.currentTimeMillis() - start);
			
			NormalizedChemaCreator.createIndexes(database.getConnection());
			System.out.println("Loading data ... finished");
			long coldStart = System.currentTimeMillis() - start;
			System.out.println("Cold start ... finished after " + (coldStart) + " miliseconds");
			
			tpcEOriginalSchema.startTransactionMixture(DataT2T3T8.pathToFile, timestamp, difference);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

}
