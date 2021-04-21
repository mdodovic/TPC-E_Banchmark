package rs.ac.bg.etf.matija.dataCreation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import rs.ac.bg.etf.matija.main.Main;
public class DataT2T3T8 {

	private Connection databaseConnection;
	
	public static final String customerPositionFile = "customer_position_mix_130k.sql";
	

	// this is also output file for data generation

	public static long totalLineCount = 0;
	public static long readLineCount = 0;
	public static long writeLineCount = 0;
	
	private long innerLineCount;
	
	public DataT2T3T8(Connection connection) {
		this.databaseConnection = connection;	

		try (Stream<String> stream = Files.lines(Paths.get(customerPositionFile), StandardCharsets.UTF_8)) {
			innerLineCount = stream.count();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("CustomerPosition transaction's : " + innerLineCount);
	}

	public String dataGenerationT2ReadOnly() {
		
		try (FileReader fr = new FileReader(customerPositionFile);
			BufferedReader br = new BufferedReader(fr);
			FileWriter fw = new FileWriter(Main.inputDataFile);
			PrintWriter pw = new PrintWriter(fw)){

			long cust_id;
			String tax_id;
			
			String s;
			while((s = br.readLine()) != null){
				String[] parsedTransaction = s.split(" ");
				if ("CustomerPositionFrame1".equals(parsedTransaction[1])) {
					String[] data = parsedTransaction[2].split(",");
					cust_id = Long.parseLong(data[0]);
					tax_id = data[1];
					
					cust_id = removeTaxId(cust_id, tax_id);

					pw.write("EXEC " + "CustomerPositionFrame1 " + 
							cust_id + "," + "" + "," + 0 + "," + -1 + "\n");					
					
					readLineCount += 1;

				}

			}

			totalLineCount = readLineCount + writeLineCount;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Write transactions: " + String.format("%.2f", 100. * writeLineCount / totalLineCount));
		System.out.println("Read transactions: " + String.format("%.2f", 100. * readLineCount / totalLineCount));

		return Main.inputDataFile;		
	}
	
	

	public String dataGenerationT3T8WriteOnly() {
		
		try (FileReader fr = new FileReader(customerPositionFile);
				BufferedReader br = new BufferedReader(fr);
				FileWriter fw = new FileWriter(Main.inputDataFile);
				PrintWriter pw = new PrintWriter(fw)){

				long cust_id;
				String tax_id;
				int cnt = 0;
				String s;
				while((s = br.readLine()) != null){
					String[] parsedTransaction = s.split(" ");
					if ("CustomerPositionFrame1".equals(parsedTransaction[1])) {
						String[] data = parsedTransaction[2].split(",");
						cust_id = Long.parseLong(data[0]);
						tax_id = data[1];
						
						DataT2T3T8.writeLineCount += generateData(cust_id, tax_id, pw);
						cnt += 1;
						//break;
					}
					if(cnt % 1000 == 0) {
						System.out.println("Finished: " + cnt + " customer lines");
					}
					//if( DataT2T3T8.writeLineCount >= 1300000)
					//	break;
				}
				
				totalLineCount = readLineCount + writeLineCount;

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("Write transactions: " + String.format("%.2f", 100. * writeLineCount / totalLineCount));
			System.out.println("Read transactions: " + String.format("%.2f", 100. * readLineCount / totalLineCount));

			
			return Main.inputDataFile;
		
	}

	public String dataGenerationT2T3T8_3() {

//		3.	N read transakcija pa onda 1 write blok ova (da se namesti da bude ~50% read/write transackija)

		try (FileReader fr = new FileReader(customerPositionFile);
			BufferedReader br = new BufferedReader(fr);
			FileWriter fw = new FileWriter(Main.inputDataFile);
			PrintWriter pw = new PrintWriter(fw)				
				){

			long cust_id;
			String tax_id;
			
			long cnt = 0;
			
			String s;
			while((s = br.readLine()) != null){
				String[] parsedTransaction = s.split(" ");
				if ("CustomerPositionFrame1".equals(parsedTransaction[1])) {
					String[] data = parsedTransaction[2].split(",");
					cust_id = Long.parseLong(data[0]);
					tax_id = data[1];
					
					long readCnt = 0;
					if(cnt % 12 == 0) {
						
						try (FileReader frInner = new FileReader(customerPositionFile);
								BufferedReader brInner = new BufferedReader(frInner);){
							
							String sInner;
							while((sInner = brInner.readLine()) != null){
								String[] parsedTransactionInner = sInner.split(" ");
								if ("CustomerPositionFrame1".equals(parsedTransactionInner[1])) {
									String[] dataInner = parsedTransactionInner[2].split(",");
									long cust_idInner = Long.parseLong(dataInner[0]);
									String tax_idInner = dataInner[1];
	
									readCnt ++;
	
									pw.write("EXEC " + "CustomerPositionFrame1 " + 
											cust_idInner + "," + tax_idInner + "," + 0 + "," + -1 + "\n");
								}
							}
	
						}
					}
					DataT2T3T8.writeLineCount += generateData(cust_id, tax_id, pw);

					DataT2T3T8.readLineCount += readCnt;
					cnt ++;
				}

			}
			
			
			totalLineCount = readLineCount + writeLineCount;
			
			
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Write transactions: " + String.format("%.2f", 100. * writeLineCount / totalLineCount));
		System.out.println("Read transactions: " + String.format("%.2f", 100. * readLineCount / totalLineCount));

		
		return Main.inputDataFile;

	}

	
	public String dataGenerationT2T3T8_2() {

//		2.	1 read transakcija – 1 write blok za tu transakciju , pa kad se sve izredjaju onda se ponove još jedno sve read transakcije.

		try (FileReader fr = new FileReader(customerPositionFile);
			BufferedReader br = new BufferedReader(fr);
			FileWriter fw = new FileWriter(Main.inputDataFile);
			PrintWriter pw = new PrintWriter(fw)				
				){

			long cust_id;
			String tax_id;
			
			String s;
			while((s = br.readLine()) != null){
				String[] parsedTransaction = s.split(" ");
				if ("CustomerPositionFrame1".equals(parsedTransaction[1])) {
					String[] data = parsedTransaction[2].split(",");
					cust_id = Long.parseLong(data[0]);
					tax_id = data[1];
					
					pw.write("EXEC " + "CustomerPositionFrame1 " + 
							cust_id + "," + tax_id + "," + 0 + "," + -1 + "\n");

					DataT2T3T8.writeLineCount += generateData(cust_id, tax_id, pw);
					
					pw.write("EXEC " + "CustomerPositionFrame1 " + 
							cust_id + "," + tax_id + "," + 0 + "," + -1 + "\n");

					DataT2T3T8.readLineCount += 2;

				}

			}
			
			
			totalLineCount = readLineCount + writeLineCount;
			
			
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Write transactions: " + String.format("%.2f", 100. * writeLineCount / totalLineCount));
		System.out.println("Read transactions: " + String.format("%.2f", 100. * readLineCount / totalLineCount));

		
		return Main.inputDataFile;

	}

	

	public String dataGenerationT2T3T8_1() {

//		1.	ceo blok read transakcija ponavljam nakon svakog pojedinacnog write blok (1 write blok je sve write transakcije koje se odnose na 1 T2 read transakciju)

		try (FileReader fr = new FileReader(customerPositionFile);
			BufferedReader br = new BufferedReader(fr);
			FileWriter fw = new FileWriter(Main.inputDataFile);
			PrintWriter pw = new PrintWriter(fw)				
				){

			long cust_id;
			String tax_id;
			
			String s;
			while((s = br.readLine()) != null){
				String[] parsedTransaction = s.split(" ");
				if ("CustomerPositionFrame1".equals(parsedTransaction[1])) {
					String[] data = parsedTransaction[2].split(",");
					cust_id = Long.parseLong(data[0]);
					tax_id = data[1];
					
					long readCnt = 0;
					long writeCnt = 0;
					
					try (FileReader frInner = new FileReader(customerPositionFile);
							BufferedReader brInner = new BufferedReader(frInner);){
						
						String sInner;
						while((sInner = brInner.readLine()) != null){
							String[] parsedTransactionInner = sInner.split(" ");
							if ("CustomerPositionFrame1".equals(parsedTransactionInner[1])) {
								String[] dataInner = parsedTransactionInner[2].split(",");
								long cust_idInner = Long.parseLong(dataInner[0]);
								String tax_idInner = dataInner[1];

								readCnt ++;

								pw.write("EXEC " + "CustomerPositionFrame1 " + 
										cust_idInner + "," + tax_idInner + "," + 0 + "," + -1 + "\n");
							}
						}

					}
					
					
					writeCnt = generateData(cust_id, tax_id, pw);
					
					DataT2T3T8.readLineCount += readCnt;
					DataT2T3T8.writeLineCount += writeCnt;
				
				}

			}
			
			totalLineCount = readLineCount + writeLineCount;
			
			
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Write transactions: " + String.format("%.2f", 100. * writeLineCount / totalLineCount));
		System.out.println("Read transactions: " + String.format("%.2f", 100. * readLineCount / totalLineCount));

		
		return Main.inputDataFile;

	}

	
	private long removeTaxId(long cust_id, String tax_id) {
		if(cust_id == 0) {
			String getIdFromTaxIdQuery = "select C_ID from CUSTOMER where C_TAX_ID = ?";
			try (PreparedStatement stmt = databaseConnection.prepareStatement(getIdFromTaxIdQuery)){
				stmt.setString(1, tax_id);
				try(ResultSet rs = stmt.executeQuery()){
				
					if(rs.next() == true) {
						cust_id = rs.getLong(1);
					}
				}
			} catch(SQLException e) { 
				e.printStackTrace(); 
			}			
		}
		return cust_id;
	}
	
	private long generateData(long cust_id, String tax_id, PrintWriter pw) {

		// Fetch data for T3 and T8 transaction:
		
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
		// cust_id !!!
		
		String getKeys = "SELECT CA_ID, LT_S_SYMB\r\n" + 
				"FROM\r\n" + 
				"	CUSTOMER_ACCOUNT left outer join\r\n" + 
				"	HOLDING_SUMMARY on HS_CA_ID = CA_ID,\r\n" + 
				"	LAST_TRADE\r\n" + 
				"where\r\n" + 
				"	CA_C_ID = ? and\r\n" + 
				"	LT_S_SYMB = HS_S_SYMB";
		
		long cnt = 0;
		
		try (PreparedStatement stmt = databaseConnection.prepareStatement(getKeys)){
			//stmt.setInt(1, Constraints.max_acct_len_rows);
			stmt.setLong(1, cust_id);
			try(ResultSet rs = stmt.executeQuery()){
				
				while(rs.next()) {
					long ca_id = rs.getLong(1);
					String lt_s_symb = rs.getString(2);
					//System.out.println(cust_id + " - " + ca_id + "  " + lt_s_symb);
					
					// MarketFeed:

					double price_quote = (Math.random() * 100) - 50.; // rand
					String pq = String.format("%.2f", price_quote);
					String status_submitted = ""; // rand
					String symbol = lt_s_symb.trim(); // !!!!!!!!!
					long trade_qty = (long)((Math.random() * 10) - 5); // rand
					String type_limit_buy = ""; // rand
					String type_limit_sell = ""; // rand
					String type_stop_loss = ""; // rand
				
					pw.write("EXEC " + "MarketFeedFrame1 " + 
							pq + "," + status_submitted + "," +  symbol 
							+ "," +  trade_qty + "," +  type_limit_buy + "," + 
							type_limit_sell + "," +  type_stop_loss + "\n");

					// TradeResult F2:		
					long acct_id = ca_id; // !!!!!!!!!!
					symbol = lt_s_symb.trim(); // !!!!!!!!!
					int hs_qty = (int)((Math.random() * 10000)); // rand
					int trade_qty_hs = (int)((Math.random() * 1000)); // rand

					pw.write("EXEC " + "TradeResultFrame2 " + 
							acct_id + "," + hs_qty + "," + symbol + "," + trade_qty_hs + "\n");
					
					
					// TradeResult F6:		
					acct_id = ca_id; // !!!!!!!!!!
					double se_amount = (Math.random() * 100000.) - (Math.random() * 25000.); // rand
					String se = String.format("%.2f", se_amount);
					//*		startTradeResult(acct_id, se_amount);
					pw.write("EXEC " + "TradeResultFrame6 " + 
							acct_id + "," + se + "\n");
					
					
					cnt += 3;
				
				}
			}
		} catch(SQLException e) { 
			e.printStackTrace(); 
		}
		
		return cnt;
		
	}
	
}
