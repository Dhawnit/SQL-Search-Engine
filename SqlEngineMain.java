import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.ESqlStatementType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class SqlEngineMain {

	public static HashMap<String, HashMap<String, ArrayList<Integer>>> tuples;
	public static HashMap<String, ArrayList<String> > tableNameAttributes;

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		tuples = new HashMap<String, HashMap<String, ArrayList<Integer>>>();
		tableNameAttributes = new HashMap<String, ArrayList<String>>();
		
		try {

			BufferedReader br = new BufferedReader(new FileReader("metadata.txt"));
			String line = br.readLine();

			while (line != null) {

				String tableName = "";

				if (line.equalsIgnoreCase("<begin_table>")) {

					tableName = br.readLine();
					ArrayList<String> columnAttributes = new ArrayList<String>();

					line = br.readLine();
					while (!line.equalsIgnoreCase("<end_table>")) {
						
						columnAttributes.add(line);
						line = br.readLine();
						
					}
					line = br.readLine();
					tableNameAttributes.put(tableName, columnAttributes);
				}
			}
			br.close();

			// ============================= Initialize tables

			for (Entry<String, ArrayList<String>> entry : tableNameAttributes.entrySet()) {

				HashMap<String, ArrayList<Integer>> temp = new HashMap <String, ArrayList<Integer>>();

				for (String attributeName : entry.getValue()) {
					ArrayList<Integer> column = new ArrayList<Integer>();
					temp.put(attributeName, column);
				}
				tuples.put(entry.getKey(), temp);
			}

			// ============================== Populate tables

			for (Entry<String, ArrayList<String>> entry : tableNameAttributes.entrySet()) {
				
				ArrayList<String> columnNames = entry.getValue();

				Map<String, ArrayList<Integer>> temp = new HashMap<String, ArrayList<Integer>>();
				temp = tuples.get(entry.getKey());


				BufferedReader br1 = new BufferedReader(new FileReader(entry.getKey() + ".csv"));
				line = br1.readLine();

				ArrayList<Integer> column;
				// System.out.println(line);

				int count=0;
				StringTokenizer st = null;
				String token=null;
				
				while (line != null) {
					
					count=0;
					st = new StringTokenizer(line, ",");

					while (st.hasMoreElements()) {
						
						token = st.nextToken();
						column = temp.get(columnNames.get(count));

						if (!token.startsWith("\"")) {

							int n = Integer.parseInt(token);
							column.add(n);
							tuples.get(entry.getKey()).put(columnNames.get(count), column);
							count++;
						} else {

							token = token.substring(1,token.length());
							int n = Integer.parseInt(token);
							column.add(n);
							tuples.get(entry.getKey()).put(columnNames.get(count), column);
							count++;
						}
						
					}
					line = br1.readLine();
				}
				br1.close();
			}

			TGSqlParser parser = new TGSqlParser(EDbVendor.dbvoracle);

//			 System.out.println(args[0]);
			 parser.sqltext = args[0];
			
			// Queries for testing
//			parser.sqltext = query;
			
			int ret = parser.parse();
			if (ret == 0) {
				
				for (int i = 0; i < parser.sqlstatements.size(); i++) {
					handleStatement(parser.sqlstatements.get(i));
				}
			} else {
				System.out.println(parser.getErrormessage());
			}

		} catch (Exception e) {
			System.out.println("Error");
			e.printStackTrace();
		}
	}

	// Check type of SQL Query and perform tasks accordingly
	public static void handleStatement(TCustomSqlStatement statement) throws Exception{

		if (statement.sqlstatementtype == ESqlStatementType.sstselect ) {		
			HandleSelectNew.handleSelectStatement((TSelectSqlStatement) statement);
		}
		else{
			System.out.println("Error : Please write Select queries only.");
		}
	}
}
