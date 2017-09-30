import java.util.*;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.commons.lang3.math.NumberUtils;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class HandleSelectNew {
	
	public static void handleSelectStatement(TSelectSqlStatement pStmt) throws Exception {

		ArrayList<String> columns = new ArrayList<String>();

		for (int i = 0; i < pStmt.getResultColumnList().size(); i++) {

			TResultColumn resultColumn = pStmt.getResultColumnList().getResultColumn(i);
			columns.add(resultColumn.getExpr().toString());
		}

		ArrayList<String> tables = new ArrayList<String>();
		
		for (int i = 0; i < pStmt.joins.size(); i++) {

			TJoin join = pStmt.joins.getJoin(i);
			tables.add(join.getTable().toString());
		}
		
//		System.out.println("Column Names: ");
//		for (String s : columns) {
//			System.out.println(s);
//		}
//
//		System.out.println("Table Names: ");
//		for (String s : tables) {
//			System.out.println(s);
//		}
//		System.out.println();
		
		
		// Error cases for tables
		for (String s : tables) {
			if(!SqlEngineMain.tableNameAttributes.containsKey(s)){
				System.out.println("Error: Table "+s+" not found");
				return;
			}
		}
		
		// Error cases for Columns		
		ArrayList<String> notFound =new ArrayList<String>();
		for(String s : columns){
			if(s.equals("*"))
				break;
			if(s.startsWith("max") || s.startsWith("min") || s.startsWith("sum") || s.startsWith("avg") || s.startsWith("distinct"))
				continue;
			if(s.contains(".")){
				String temp1[]=s.split("\\.");
				if(SqlEngineMain.tableNameAttributes.containsKey(temp1[0])){
					if(!SqlEngineMain.tableNameAttributes.get(temp1[0]).contains(temp1[1])){
						System.out.println("Error: Column "+temp1[1]+" doesnot exists in table "+temp1[0]);
						return;
					}	
				}else{
					System.out.println("Error: Table "+temp1[0]+" not found");
					return;
				}
			}else{
				int eflag = 0;
				for (Map.Entry<String, ArrayList<String>> entry : SqlEngineMain.tableNameAttributes.entrySet()) {
					ArrayList<String> value = entry.getValue();
					if(value.contains(s)){
						eflag=1;
						break;
					}		    	
				}
				if(eflag==0)
					notFound.add(s);
			}
		}
		if(notFound.size()!=0){
			System.out.println("Error: Column "+notFound.toString()+" doesnot exists");
			return;
		}

		
		ArrayList<String> resultAttributes = new ArrayList<String>();
		ArrayList<String> resultAttributesTable = new ArrayList<String>();
		ArrayList<ArrayList<Integer>> resultData = new ArrayList<ArrayList<Integer>>();
		
		if(tables.size()==3){ // Cross join of 3 tables
			for(String s : tables){
				for(String k : SqlEngineMain.tableNameAttributes.get(s)){
					resultAttributes.add(k);
					resultAttributesTable.add(s);
				}
			}
			int table1Count = SqlEngineMain.tuples.get(tables.get(0)).get(SqlEngineMain.tableNameAttributes.get(tables.get(0)).get(0)).size();
			int table2Count = SqlEngineMain.tuples.get(tables.get(1)).get(SqlEngineMain.tableNameAttributes.get(tables.get(1)).get(0)).size();
			int table3Count = SqlEngineMain.tuples.get(tables.get(2)).get(SqlEngineMain.tableNameAttributes.get(tables.get(2)).get(0)).size();
			ArrayList<Integer> temp = new ArrayList<Integer>();
			
			for(int i=0; i < table1Count ;i++){
				
				for (int j=0; j < table2Count;j++){

					for (int k=0; k < table3Count;k++){					

						for(String s : SqlEngineMain.tableNameAttributes.get(tables.get(0)))
							temp.add(SqlEngineMain.tuples.get(tables.get(0)).get(s).get(i));
						for(String s : SqlEngineMain.tableNameAttributes.get(tables.get(1)))
							temp.add(SqlEngineMain.tuples.get(tables.get(1)).get(s).get(j));
						for(String s : SqlEngineMain.tableNameAttributes.get(tables.get(2)))
							temp.add(SqlEngineMain.tuples.get(tables.get(2)).get(s).get(k));
						resultData.add(temp);

						temp = new ArrayList<Integer>();
					}
				}
			}
		}
		else if(tables.size()==2){	// Cross join of 2 tables

			for(String s : tables){
				for(String k : SqlEngineMain.tableNameAttributes.get(s)){
					resultAttributes.add(k);
					resultAttributesTable.add(s);
				}
			}
			int table1Count = SqlEngineMain.tuples.get(tables.get(0)).get(SqlEngineMain.tableNameAttributes.get(tables.get(0)).get(0)).size();
			int table2Count = SqlEngineMain.tuples.get(tables.get(1)).get(SqlEngineMain.tableNameAttributes.get(tables.get(1)).get(0)).size();
			ArrayList<Integer> temp = new ArrayList<Integer>();
			
			for(int i=0; i < table1Count ;i++){
				
				for (int j=0; j < table2Count;j++){
					
					for(String s : SqlEngineMain.tableNameAttributes.get(tables.get(0)))
						temp.add(SqlEngineMain.tuples.get(tables.get(0)).get(s).get(i));
					
					for(String s : SqlEngineMain.tableNameAttributes.get(tables.get(1)))
						temp.add(SqlEngineMain.tuples.get(tables.get(1)).get(s).get(j));
					resultData.add(temp);
					
					temp = new ArrayList<Integer>();
				}
			}		
		}
		else { // Only one table

			for(String s : tables){
				for(String k : SqlEngineMain.tableNameAttributes.get(s)){
					resultAttributes.add(k);
					resultAttributesTable.add(s);
				}
			}
			ArrayList<Integer> temp = new ArrayList<Integer>();
			int table1Count = SqlEngineMain.tuples.get(tables.get(0)).get(SqlEngineMain.tableNameAttributes.get(tables.get(0)).get(0)).size();
			for(int i=0; i < table1Count ;i++){
				for(String s : SqlEngineMain.tableNameAttributes.get(tables.get(0)))
					temp.add(SqlEngineMain.tuples.get(tables.get(0)).get(s).get(i));
				resultData.add(temp);
				temp = new ArrayList<Integer>();
			}
		}
		
		ArrayList<Boolean> mark = new ArrayList<Boolean>();
		
		if (pStmt.getWhereClause() != null) { // Filter rows

			String whereClause = pStmt.getWhereClause().toString();
//			System.out.println(whereClause);
			StringTokenizer st= new StringTokenizer(whereClause);
			st.nextToken();
			ArrayList<String> operations = new ArrayList<String>();
			ArrayList<String> expressions = new ArrayList<String>();
			
			while(st.hasMoreTokens()){
				String temp = st.nextToken();
				if(temp.equalsIgnoreCase("and"))
					operations.add("and");
				else if(temp.equalsIgnoreCase("or"))
					operations.add("or");
				else
					expressions.add(temp);
			}
//			System.out.println(operations);
//			System.out.println(expressions);
			
			// Error cases in where clause
			for(String e : expressions){
				
				String[] temp=null;
				
				if(e.contains("=")){
					temp = e.split("=");
				}
				if(e.contains("<")){
					temp = e.split("<");
				}
				if(e.contains(">")){
					temp = e.split(">");
				}
				if(e.contains(">=")){
					temp = e.split(">=");
				}
				if(e.contains("<=")){
					temp = e.split("<=");
				}
				if(e.contains("!=")){
					temp = e.split("!=");
				}
				
				if(temp[0].contains(".")){
					String temp1[]=temp[0].split("\\.");
					if(SqlEngineMain.tableNameAttributes.containsKey(temp1[0])){
						if(!SqlEngineMain.tableNameAttributes.get(temp1[0]).contains(temp1[1])){
							System.out.println("Error: Column "+temp1[1]+" doesnot exists in table "+temp1[0]);
							return;
						}	
					}else{
						System.out.println("Error: Table "+temp1[0]+" not found");
						return;
					}
				}
			}
			
			for (int i=0;i<resultData.size();i++)
				mark.add(false);
			
			while(!expressions.isEmpty()){

				if(operations.contains("and")){

					int index = operations.indexOf("and");

					if(expressions.size()==1){

						String expr1 = expressions.get(index);
						operations.remove(index);
						expressions.remove(index);
						String tableName1 = "", attribute1 ="", tableName2="", attribute2="";
						int indexOfAttribute11 = 0;
						int valueOfAttribute11 = 0;
						
						String[] temp=null;
						String expr1op = null;
						
						if(expr1.contains("=")){
							temp = expr1.split("=");
							expr1op = "=";
						}
						if(expr1.contains("<")){
							temp = expr1.split("<");
							expr1op = "<";
						}
						if(expr1.contains(">")){
							temp = expr1.split(">");
							expr1op = ">";
						}
						if(expr1.contains(">=")){
							temp = expr1.split(">=");
							expr1op = ">=";
						}
						if(expr1.contains("<=")){
							temp = expr1.split("<=");
							expr1op = "<=";
						}
						if(expr1.contains("!=")){
							temp = expr1.split("!=");
							expr1op = "!=";
						}
						
						temp[0]=temp[0].trim();
						if(temp[0].contains(".")){
							String[] temp1 = temp[0].split("\\.");
							tableName1 = temp1[0];
							attribute1 = temp1[1];
							temp[0]=attribute1;
							for(int i =0 ;i < resultAttributes.size(); i++)
								if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
									indexOfAttribute11 = i;
									break;
								}
							}else{
								indexOfAttribute11 = resultAttributes.indexOf(temp[0].trim());
							}
							
							temp[1]=temp[1].trim();
							if(temp[1].contains(".")){
								String temp1[] = temp[1].split("\\.");
								tableName2 = temp1[0];
								attribute2 = temp1[1];
								temp[1]=attribute2;
							}
							
							if(NumberUtils.isNumber(temp[1])){
								valueOfAttribute11 = Integer.parseInt(temp[1].trim());
								for(int i = 0; i < resultData.size();i++){
									ArrayList<Integer> temp2 = resultData.get(i);
									String expression1 = null;
									if(expr1op.equals("="))
										expression1 = temp2.get(indexOfAttribute11)+expr1op+expr1op+valueOfAttribute11;
									else
										expression1 = temp2.get(indexOfAttribute11)+expr1op+valueOfAttribute11;
									
									ScriptEngineManager manager = new ScriptEngineManager();
									ScriptEngine engine = manager.getEngineByName("js");
									Object result1 = engine.eval(expression1);
									if(!(result1.toString().equals("true") && mark.get(i)==true))
										mark.set(i, false);
								}
							}else{
								int indexOfAttribute2=0;
								for(int i =0 ;i < resultAttributes.size(); i++){
									if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
										indexOfAttribute2 = i;
										break;
									}
								}
								
								for(int i = 0; i < resultData.size();i++){

									ArrayList<Integer> temp2 = resultData.get(i);
									int x = temp2.get(indexOfAttribute11);
									int y = temp2.get(indexOfAttribute2);

									// if(!(expr1op.equals("=") && x==y && mark.get(i))){
									// 	mark.set(i, true);
									// }
									// if(!(expr1op.equals("<") && x<y && mark.get(i))){
									// 	mark.set(i, true);
									// }
									// if(!(expr1op.equals(">") && x>y && mark.get(i))){
									// 	mark.set(i, true);
									// }
									// if(!(expr1op.equals("<=") && x<=y && mark.get(i))){
									// 	mark.set(i, true);
									// }
									// if(!(expr1op.equals(">=") && x>=y && mark.get(i))){
									// 	mark.set(i, true);
									// }
									// if(!(expr1op.equals("!=") && x!=y && mark.get(i))){
									// 	mark.set(i, true);
									// }

									if(expr1op.equals("=")){

										if(x!=y && mark.get(i)) {
											mark.set(i, false);
										}
									}
									if(expr1op.equals("<")){

										if((x>=y) && mark.get(i)) {
											mark.set(i, false);
										}
									}
									if(expr1op.equals(">")){

										if((x<=y)  && mark.get(i)) {
											mark.set(i, false);
										}
									}
									if(expr1op.equals(">=")){
										if((x<y)  && mark.get(i)) {
											mark.set(i, false);
										}
									}
									if(expr1op.equals("<=")){
										if((x>y) && mark.get(i)) {
											mark.set(i, false);
										}
									}
									if(expr1op.equals("!=")){
										if((x==y) && mark.get(i)) {
											mark.set(i, false);
										}
									}

								}
							}
						}else{
							
							String expr1 = expressions.get(index);
							String expr2 = expressions.get(index+1);
							operations.remove(index);
							expressions.remove(index);
							expressions.remove(index);
							
							String[] temp=null;
							String expr1op = null;
							if(expr1.contains("=")){
								temp = expr1.split("=");
								expr1op = "=";
							}
							if(expr1.contains("<")){
								temp = expr1.split("<");
								expr1op = "<";
							}
							if(expr1.contains(">")){
								temp = expr1.split(">");
								expr1op = ">";
							}
							if(expr1.contains(">=")){
								temp = expr1.split(">=");
								expr1op = ">=";
							}
							if(expr1.contains("<=")){
								temp = expr1.split("<=");
								expr1op = "<=";
							}
							if(expr1.contains("!=")){
								temp = expr1.split("!=");
								expr1op = "!=";
							}
//							System.out.println(expr1op);
							
							String tableName1 = "", attribute1 ="", tableName2="", attribute2="";
							int indexOfAttribute11 = 0;
							int valueOfAttribute11 = 0;
							int indexOfAttribute21 = 0;
							int valueOfAttribute21 = 0;
							int flag1=0,flag2=0;
							
							temp[0]=temp[0].trim();
							if(temp[0].contains(".")){

								String[] temp1 = temp[0].split("\\.");
								tableName1 = temp1[0];
								attribute1 = temp1[1];
								temp[0]=attribute1;
								for(int i =0 ;i < resultAttributes.size(); i++)
									if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
										indexOfAttribute11 = i;
										break;
									}
								}else{
									indexOfAttribute11 = resultAttributes.indexOf(temp[0].trim());
								}
//								System.out.println(indexOfAttribute11);
								temp[1]=temp[1].trim();
								if(temp[1].contains(".")){
									String temp1[] = temp[1].split("\\.");
									tableName2 = temp1[0];
									attribute2 = temp1[1];
									temp[1]=attribute2;
								}
								if(NumberUtils.isNumber(temp[1])){
									valueOfAttribute11 = Integer.parseInt(temp[1].trim());
									flag1 = 1;
									// System.out.println("flag 1 set value of attribute "+attribute1+" is "+ valueOfAttribute11);
								}else{
									
									int indexOfAttribute2=0;
									for(int i =0 ;i < resultAttributes.size(); i++){
										if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
											indexOfAttribute2 = i;
											break;
										}
									}
									
									for(int i = 0; i < resultData.size();i++){
										
										ArrayList<Integer> temp2 = resultData.get(i);
										int x = temp2.get(indexOfAttribute11);
										int y = temp2.get(indexOfAttribute2);
										
										if(expr1op.equals("=") && x==y){
											mark.set(i, true);
										}
										if(expr1op.equals("<") && x<y){
											mark.set(i, true);
										}
										if(expr1op.equals(">") && x>y){
											mark.set(i, true);
										}
										if(expr1op.equals("<=") && x<=y){
											mark.set(i, true);
										}
										if(expr1op.equals(">=") && x>=y){
											mark.set(i, true);
										}
										if(expr1op.equals("!=") && x!=y){
											mark.set(i, true);
										}
									}
									// resultAttributes.remove(indexOfAttribute2);
									// resultAttributesTable.remove(indexOfAttribute2);
									// for(int i=0;i<resultData.size();i++){
									// 	resultData.get(i).remove(indexOfAttribute2);
									// }
								}
								
								String[] temp1 = null;
								String expr2op = null;
								if(expr2.contains("=")){
									temp1 = expr2.split("=");
									expr2op = "=";
								}
								if(expr2.contains("<")){
									temp1 = expr2.split("<");
									expr2op = "<";
								}
								if(expr2.contains(">")){
									temp1 = expr2.split(">");
									expr2op = ">";
								}
								if(expr2.contains(">=")){
									temp1 = expr2.split(">=");
									expr2op = ">=";
								}
								if(expr2.contains("<=")){
									temp1 = expr2.split("<=");
									expr2op = "<=";
								}
								if(expr2.contains("!=")){
									temp1 = expr2.split("!=");
									expr2op = "!=";
								}
//								System.out.println(expr2op);
								tableName1 = attribute1 = tableName2 = attribute2= "";
								
								temp1[0]=temp1[0].trim();
								if(temp1[0].contains(".")){
									
									String[] temp2 = temp1[0].split("\\.");
									tableName1 = temp2[0];
									attribute1 = temp2[1];
									temp1[0]=attribute1;
									for(int i =0 ;i < resultAttributes.size(); i++)
										if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
											indexOfAttribute21 = i;
											break;
										}
										
									}else{
										indexOfAttribute21 = resultAttributes.indexOf(temp1[0].trim());
									}
//									System.out.println(indexOfAttribute21);
									temp1[1]=temp1[1].trim();
									
									if(temp1[1].contains(".")){
										String temp2[] = temp1[1].split("\\.");
										tableName2 = temp2[0];
										attribute2 = temp2[1];
										temp1[1]=attribute2;
									}
									
									if(NumberUtils.isNumber(temp1[1])){
										valueOfAttribute21 = Integer.parseInt(temp1[1].trim());
										flag2=1;
										// System.out.println("flag 2 set value of attribute "+ valueOfAttribute21);
									}else{
										int indexOfAttribute2=0;
										for(int i =0 ;i < resultAttributes.size(); i++){
											if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
												indexOfAttribute2 = i;
												break;
											}
										}
										for(int i = 0; i < resultData.size();i++){

											ArrayList<Integer> temp2 = resultData.get(i);
											int x = temp2.get(indexOfAttribute21);
											int y = temp2.get(indexOfAttribute2);
											
											if(expr2op.equals("=")){
												if(x==y && flag1==1){
													mark.set(i, true);
												}else if(x!=y && flag1==1){
													mark.set(i, false);
												}
												else if(x!=y && flag1==0 && mark.get(i)) {
													mark.set(i, false);
												}
											}
											if(expr2op.equals("<")){
												if(x<y && flag1==1){
													mark.set(i, true);
												}else if( (x>=y) && flag1==1){
													mark.set(i, false);
												}
												else if((x>=y) && flag1==0 && mark.get(i)) {
													mark.set(i, false);
												}
											}
											if(expr2op.equals(">")){
												if(x>y && flag1==1){
													mark.set(i, true);
												}else if((x<=y) && flag1==1){
													mark.set(i, false);
												}
												else if((x<=y) && flag1==0 && mark.get(i)) {
													mark.set(i, false);
												}
											}
											if(expr2op.equals(">=")){
												if(x>=y && flag1==1){
													mark.set(i, true);
												}else if((x<y) && flag1==1){
													mark.set(i, false);
												}
												else if((x<y) && flag1==0 && mark.get(i)) {
													mark.set(i, false);
												}
											}
											if(expr2op.equals("<=")){
												if(x<=y && flag1==1){
													mark.set(i, true);
												}else if((x>y) && flag1==1){
													mark.set(i, false);
												}
												else if((x>y) && flag1==0 && mark.get(i)) {
													mark.set(i, false);
												}
											}
											if(expr2op.equals("!=")){
												if(x!=y && flag1==1){
													mark.set(i, true);
												}else if((x==y) && flag1==1){
													mark.set(i, false);
												}
												else if((x==y) && flag1==0 && mark.get(i)) {
													mark.set(i, false);
												}
											}
											// if(expr2op.equals("=") && x==y && flag1==1){
											// 	mark.set(i, true);
											// }else if(!(expr2op.equals("=") && x==y && mark.get(i))) {
											// 	mark.set(i, false);
											// }else if(expr2op.equals("<") && x<y  && flag1==1){
											// 	mark.set(i, true);
											// }else if(!(expr2op.equals("<") && x<y && mark.get(i))){
											// 	mark.set(i, false);
											// }else if(expr2op.equals(">") && x>y  && flag1==1){
											// 	mark.set(i, true);
											// }else if(!(expr2op.equals(">") && x>y && mark.get(i))){
											// 	mark.set(i, false);
											// }else if(expr2op.equals("<=") && x<=y  && flag1==1){
											// 	mark.set(i, true);
											// }else if(!(expr2op.equals("<=") && x<=y && mark.get(i))){
											// 	mark.set(i, false);
											// }else if(expr2op.equals(">=") && x>=y  && flag1==1){
											// 	mark.set(i, true);
											// }else if(!(expr2op.equals(">=") && x>=y && mark.get(i))){
											// 	mark.set(i, false);
											// }else if(expr2op.equals("!=") && x!=y  && flag1==1){
											// 	mark.set(i, true);
											// }else if(!(expr2op.equals("!=") && x!=y && mark.get(i))){
											// 	mark.set(i, false);
											// }
										}
									}
									
									if(flag1==1 && flag2==1){

										for(int i = 0; i < resultData.size();i++){
											
											ArrayList<Integer> temp2 = resultData.get(i);
											String expression1 = null;
											if(expr1op.equals("="))
												expression1 = temp2.get(indexOfAttribute11)+expr1op+expr1op+valueOfAttribute11;
											else
												expression1 = temp2.get(indexOfAttribute11)+expr1op+valueOfAttribute11;
											
											String expression2 = null;
											if(expr2op.equals("="))
												expression2 = temp2.get(indexOfAttribute21)+expr2op+expr2op+valueOfAttribute21;
											else
												expression2 = temp2.get(indexOfAttribute21)+expr2op+valueOfAttribute21;
											
//											System.out.println(expression1);
//											System.out.println(expression2);
											
											ScriptEngineManager manager = new ScriptEngineManager();
											ScriptEngine engine = manager.getEngineByName("js");
											Object result1 = engine.eval(expression1);
											Object result2 = engine.eval(expression2);
											if(result1.toString().equals("true") && result2.toString().equals("true"))
												mark.set(i, true);
										}
									}else if(flag1==1){
										for(int i = 0; i < resultData.size();i++){
											ArrayList<Integer> temp2 = resultData.get(i);
											String expression1 = null;
											if(expr1op.equals("="))
												expression1 = temp2.get(indexOfAttribute11)+expr1op+expr1op+valueOfAttribute11;
											else
												expression1 = temp2.get(indexOfAttribute11)+expr1op+valueOfAttribute11;
											ScriptEngineManager manager = new ScriptEngineManager();
											ScriptEngine engine = manager.getEngineByName("js");
											Object result1 = engine.eval(expression1);
											if(!(result1.toString().equals("true") && mark.get(i)==true))
												mark.set(i, false);
										}
									}else if(flag2==1){
										for(int i = 0; i < resultData.size();i++){
											ArrayList<Integer> temp2 = resultData.get(i);
											String expression2 = null;
											if(expr2op.equals("="))
												expression2 = temp2.get(indexOfAttribute21)+expr2op+expr2op+valueOfAttribute21;
											else
												expression2 = temp2.get(indexOfAttribute21)+expr2op+valueOfAttribute21;
											ScriptEngineManager manager = new ScriptEngineManager();
											ScriptEngine engine = manager.getEngineByName("js");
											Object result2 = engine.eval(expression2);
											if(!(result2.toString().equals("true") && mark.get(i)==true))
												mark.set(i, false);
										}
									}
								}
							}
							else if(operations.contains("or")){

								int index = operations.indexOf("or");
								
								if(expressions.size()==1){

									String expr1 = expressions.get(index);
									operations.remove(index);
									expressions.remove(index);
									String tableName1 = "", attribute1 ="", tableName2="", attribute2="";
									int indexOfAttribute11 = 0;
									int valueOfAttribute11 = 0;
									String[] temp=null;
									String expr1op = null;
									if(expr1.contains("=")){
										temp = expr1.split("=");
										expr1op = "=";
									}
									if(expr1.contains("<")){
										temp = expr1.split("<");
										expr1op = "<";
									}
									if(expr1.contains(">")){
										temp = expr1.split(">");
										expr1op = ">";
									}
									if(expr1.contains(">=")){
										temp = expr1.split(">=");
										expr1op = ">=";
									}
									if(expr1.contains("<=")){
										temp = expr1.split("<=");
										expr1op = "<=";
									}
									if(expr1.contains("!=")){
										temp = expr1.split("!=");
										expr1op = "!=";
									}
									
									temp[0]=temp[0].trim();
									if(temp[0].contains(".")){
										String[] temp1 = temp[0].split("\\.");
										tableName1 = temp1[0];
										attribute1 = temp1[1];
										temp[0]=attribute1;
										for(int i =0 ;i < resultAttributes.size(); i++)
											if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
												indexOfAttribute11 = i;
												break;
											}
										}else{
											indexOfAttribute11 = resultAttributes.indexOf(temp[0].trim());
										}
										
										temp[1]=temp[1].trim();
										if(temp[1].contains(".")){
											String temp1[] = temp[1].split("\\.");
											tableName2 = temp1[0];
											attribute2 = temp1[1];
											temp[1]=attribute2;
										}
										
										if(NumberUtils.isNumber(temp[1])){
											valueOfAttribute11 = Integer.parseInt(temp[1].trim());
											for(int i = 0; i < resultData.size();i++){
												ArrayList<Integer> temp2 = resultData.get(i);
												String expression1 = null;
												if(expr1op.equals("="))
													expression1 = temp2.get(indexOfAttribute11)+expr1op+expr1op+valueOfAttribute11;
												else
													expression1 = temp2.get(indexOfAttribute11)+expr1op+valueOfAttribute11;
												
												ScriptEngineManager manager = new ScriptEngineManager();
												ScriptEngine engine = manager.getEngineByName("js");
												Object result1 = engine.eval(expression1);
												if(result1.toString().equals("true"))
													mark.set(i, true);
											}
										}else{
											int indexOfAttribute2=0;
											for(int i =0 ;i < resultAttributes.size(); i++){
												if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
													indexOfAttribute2 = i;
													break;
												}
											}
											
											for(int i = 0; i < resultData.size();i++){
												
												ArrayList<Integer> temp2 = resultData.get(i);
												int x = temp2.get(indexOfAttribute11);
												int y = temp2.get(indexOfAttribute2);
												
												if(expr1op.equals("=") && x==y){
													mark.set(i, true);
												}
												if(expr1op.equals("<") && x<y){
													mark.set(i, true);
												}
												if(expr1op.equals(">") && x>y){
													mark.set(i, true);
												}
												if(expr1op.equals("<=") && x<=y){
													mark.set(i, true);
												}
												if(expr1op.equals(">=") && x>=y){
													mark.set(i, true);
												}
												if(expr1op.equals("!=") && x!=y){
													mark.set(i, true);
												}
											}
										}
									}else{
										String expr1 = expressions.get(index);
										String expr2 = expressions.get(index+1);
										operations.remove(index);
										expressions.remove(index);
										expressions.remove(index);
										
										String[] temp=null;
										String expr1op = null;
										if(expr1.contains("=")){
											temp = expr1.split("=");
											expr1op = "==";
										}
										if(expr1.contains("<")){
											temp = expr1.split("<");
											expr1op = "<";
										}
										if(expr1.contains(">")){
											temp = expr1.split(">");
											expr1op = ">";
										}
										if(expr1.contains(">=")){
											temp = expr1.split(">=");
											expr1op = ">=";
										}
										if(expr1.contains("<=")){
											temp = expr1.split("<=");
											expr1op = "<=";
										}
										if(expr1.contains("!=")){
											temp = expr1.split("!=");
											expr1op = "!=";
										}
										
										String tableName1 = "", attribute1 ="", tableName2="", attribute2="";
										int indexOfAttribute11 = 0;
										int valueOfAttribute11 = 0;
										int indexOfAttribute21 = 0;
										int valueOfAttribute21 = 0;
										int flag1=0,flag2=0;
										
										temp[0]=temp[0].trim();
										if(temp[0].contains(".")){
											String[] temp1 = temp[0].split("\\.");
											tableName1 = temp1[0];
											attribute1 = temp1[1];
											temp[0]=attribute1;
											for(int i =0 ;i < resultAttributes.size(); i++)
												if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
													indexOfAttribute11 = i;
													break;
												}
											}else{
												indexOfAttribute11 = resultAttributes.indexOf(temp[0].trim());
											}
											
											temp[1]=temp[1].trim();
											if(temp[1].contains(".")){
												String temp1[] = temp[1].split("\\.");
												tableName2 = temp1[0];
												attribute2 = temp1[1];
												temp[1]=attribute2;
											}
											if(NumberUtils.isNumber(temp[1])){
												valueOfAttribute11 = Integer.parseInt(temp[1].trim());
												flag1 = 1;
											}else{
												
												int indexOfAttribute2=0;
												for(int i =0 ;i < resultAttributes.size(); i++){
													if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
														indexOfAttribute2 = i;
														break;
													}
												}
												for(int i = 0; i < resultData.size();i++){
													
													ArrayList<Integer> temp2 = resultData.get(i);
													int x = temp2.get(indexOfAttribute11);
													int y = temp2.get(indexOfAttribute2);
													
													if(expr1op.equals("==") && x==y){
														mark.set(i, true);
													}
													if(expr1op.equals("<") && x<y){
														mark.set(i, true);
													}
													if(expr1op.equals(">") && x>y){
														mark.set(i, true);
													}
													if(expr1op.equals("<=") && x<=y){
														mark.set(i, true);
													}
													if(expr1op.equals(">=") && x>=y){
														mark.set(i, true);
													}
													if(expr1op.equals("!=") && x!=y){
														mark.set(i, true);
													}
												}
											}
											
											String[] temp1 = null;
											String expr2op = null;
											if(expr2.contains("=")){
												temp1 = expr2.split("=");
												expr2op = "==";
											}
											if(expr2.contains("<")){
												temp1 = expr2.split("<");
												expr2op = "<";
											}
											if(expr2.contains(">")){
												temp1 = expr2.split(">");
												expr2op = ">";
											}
											if(expr2.contains(">=")){
												temp1 = expr2.split(">=");
												expr2op = ">=";
											}
											if(expr2.contains("<=")){
												temp1 = expr2.split("<=");
												expr2op = "<=";
											}
											if(expr2.contains("!=")){
												temp1 = expr2.split("!=");
												expr2op = "!=";
											}
											
											tableName1 = attribute1 = tableName2 = attribute2= "";
											
											temp1[0]=temp1[0].trim();
											if(temp1[0].contains(".")){
												
												String[] temp2 = temp1[0].split("\\.");
												tableName1 = temp2[0];
												attribute1 = temp2[1];
												temp1[0]=attribute1;
												for(int i =0 ;i < resultAttributes.size(); i++)
													if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
														indexOfAttribute21 = i;
														break;
													}
												}else{
													indexOfAttribute21 = resultAttributes.indexOf(temp1[0].trim());
												}
												
												temp1[1]=temp1[1].trim();
												if(temp1[1].contains(".")){
													
													String temp2[] = temp1[1].split("\\.");
													tableName2 = temp2[0];
													attribute2 = temp2[1];
													temp1[1]=attribute2;
												}
												if(NumberUtils.isNumber(temp1[1])){
													valueOfAttribute21 = Integer.parseInt(temp1[1].trim());
													flag2=1;
												}else{
													int indexOfAttribute2=0;
													for(int i =0 ;i < resultAttributes.size(); i++){
														if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
															indexOfAttribute2 = i;
															break;
														}
													}
													for(int i = 0; i < resultData.size();i++){
														
														ArrayList<Integer> temp2 = resultData.get(i);
														int x = temp2.get(indexOfAttribute21);
														int y = temp2.get(indexOfAttribute2);

														if(expr2op.equals("==") && x==y){
															mark.set(i, true);
														}
														if(expr2op.equals("<") && x<y){
															mark.set(i, true);
														}
														if(expr2op.equals(">") && x>y){
															mark.set(i, true);
														}
														if(expr2op.equals("<=") && x<=y){
															mark.set(i, true);
														}
														if(expr2op.equals(">=") && x>=y){
															mark.set(i, true);
														}
														if(expr2op.equals("!=") && x!=y){
															mark.set(i, true);
														}
													}
												}
												
												if(flag1==1 && flag2==1){
													for(int i = 0; i < resultData.size();i++){
														ArrayList<Integer> temp2 = resultData.get(i);
														String expression1 = temp2.get(indexOfAttribute11)+expr1op+valueOfAttribute11;
														String expression2 = temp2.get(indexOfAttribute21)+expr2op+valueOfAttribute21;
														ScriptEngineManager manager = new ScriptEngineManager();
														ScriptEngine engine = manager.getEngineByName("js");
														Object result1 = engine.eval(expression1);
														Object result2 = engine.eval(expression2);
														if(result1.toString().equals("true") || result2.toString().equals("true"))
															mark.set(i, true);
													}
												}else if(flag1==1){
													for(int i = 0; i < resultData.size();i++){
														ArrayList<Integer> temp2 = resultData.get(i);
														String expression1 = temp2.get(indexOfAttribute11)+expr1op+valueOfAttribute11;
														ScriptEngineManager manager = new ScriptEngineManager();
														ScriptEngine engine = manager.getEngineByName("js");
														Object result1 = engine.eval(expression1);
														if(result1.toString().equals("true"))
															mark.set(i, true);
													}
												}else if(flag2==1){
													for(int i = 0; i < resultData.size();i++){
														ArrayList<Integer> temp2 = resultData.get(i);
														String expression2 = temp2.get(indexOfAttribute21)+expr2op+valueOfAttribute21;
														ScriptEngineManager manager = new ScriptEngineManager();
														ScriptEngine engine = manager.getEngineByName("js");
														Object result2 = engine.eval(expression2);
														if(result2.toString().equals("true"))
															mark.set(i, true);
													}
												}
											}
										}
										else if(operations.isEmpty()){
											
											String expr1 = expressions.get(0);
											expressions.remove(0);
											String[] temp=null;
											String expr1op = null;
											
											if(expr1.contains("=")){
												temp = expr1.split("=");
												expr1op = "=";
											}
											if(expr1.contains("<")){
												temp = expr1.split("<");
												expr1op = "<";
											}
											if(expr1.contains(">")){
												temp = expr1.split(">");
												expr1op = ">";
											}
											if(expr1.contains(">=")){
												temp = expr1.split(">=");
												expr1op = ">=";
											}
											if(expr1.contains("<=")){
												temp = expr1.split("<=");
												expr1op = "<=";
											}
											if(expr1.contains("!=")){
												temp = expr1.split("!=");
												expr1op = "!=";
											}
											
											temp[0]=temp[0].trim();
											
											String tableName1 = "", attribute1 ="", tableName2="", attribute2="";
											int indexOfAttribute1 = 0;
											
											if(temp[0].contains(".")){
												
												String[] temp1 = temp[0].split("\\.");
												tableName1 = temp1[0];
												attribute1 = temp1[1];
												temp[0]=attribute1;
												for(int i =0 ;i < resultAttributes.size(); i++)
													if(resultAttributes.get(i).equals(attribute1) && resultAttributesTable.get(i).equals(tableName1)){
														indexOfAttribute1 = i;
														break;
													}
												}else{
													indexOfAttribute1 = resultAttributes.indexOf(temp[0].trim());
												}
												
												temp[1]=temp[1].trim();
												if(temp[1].contains(".")){
													
													String temp1[] = temp[1].split("\\.");
													tableName2 = temp1[0];
													attribute2 = temp1[1];
													temp[1]=attribute2;
												}
												
												if(NumberUtils.isNumber(temp[1])){
													
													int valueOfAttribute1 = Integer.parseInt(temp[1].trim());	
													for(int i = 0; i < resultData.size();i++){
														ArrayList<Integer> temp2 = resultData.get(i);
														String expression1 = null;
														if(expr1op.equals("="))
															expression1 = temp2.get(indexOfAttribute1)+expr1op+expr1op+valueOfAttribute1;
														else
															expression1 = temp2.get(indexOfAttribute1)+expr1op+valueOfAttribute1;
														ScriptEngineManager manager = new ScriptEngineManager();
														ScriptEngine engine = manager.getEngineByName("js");
														Object result1 = engine.eval(expression1);
														if(result1.toString().equals("true"))
															mark.set(i, true);
													}
												}else{
													int indexOfAttribute2=0;
													for(int i =0 ;i < resultAttributes.size(); i++){
														if(resultAttributes.get(i).equals(attribute2) && resultAttributesTable.get(i).equals(tableName2)){
															indexOfAttribute2 = i;
															break;
														}
													}
													
													for(int i = 0; i < resultData.size();i++){
														
														ArrayList<Integer> temp2 = resultData.get(i);
														int x = temp2.get(indexOfAttribute1);
														int y = temp2.get(indexOfAttribute2);
														
														if(expr1op.equals("=") && x==y){
															mark.set(i, true);
														}
														if(expr1op.equals("<") && x<y){
															mark.set(i, true);
														}
														if(expr1op.equals(">") && x>y){
															mark.set(i, true);
														}
														if(expr1op.equals("<=") && x<=y){
															mark.set(i, true);
														}
														if(expr1op.equals(">=") && x>=y){
															mark.set(i, true);
														}
														if(expr1op.equals("!=") && x!=y){
															mark.set(i, true);
														}
													}
													if(expr1op.equals("=")){
														resultAttributes.remove(indexOfAttribute2);
														resultAttributesTable.remove(indexOfAttribute2);
														for(int i=0;i<resultData.size();i++){
															resultData.get(i).remove(indexOfAttribute2);
														}
													}
												}
				} // end of if for single expression
			}// end of while loop for expressions
		}//end of where clause
		
		else{ // Marking all rows true since no where clause
			for (int i=0;i<resultData.size();i++)
				mark.add(true);
		}
//		for(boolean b : mark)
//			System.out.println(b);
		
		int aggregateFlag = 0;
		LinkedHashMap<String, String> aggregate = new LinkedHashMap<String, String>();
		
		for(String s: columns){
			
			if(s.startsWith("max")){
//				System.out.println("Inside Aggregate max");
				int max=Integer.MIN_VALUE;
				int index1=s.indexOf('(');
				int index2=s.lastIndexOf(')');
				int index = -1;
				int flag=0;
				
				String attribute =s.substring(index1+1, index2);
				attribute = attribute.trim();
				
				String temp1[]=null;
				if(resultAttributes.contains(attribute))
					index=resultAttributes.indexOf(attribute);
				else{
					
					for(String s1 : resultAttributesTable){
						if(attribute.startsWith(s1)){
							temp1 = attribute.split("\\.");
							if(!resultAttributesTable.contains(temp1[0])){
								System.out.println("Error: table "+temp1[0]+" doesnot exists");
								return;
							}
							for(String s2: resultAttributes){
								if(s2.equals(temp1[1]) && temp1[0].equals(s1)){
									index=resultAttributes.indexOf(s2);
									flag=1;
									break;
								}
							}
						}
						if(flag==1)
							break;
					}
				}
				if(index==-1){
					System.out.println("Error: column "+temp1[1]+" doesnot exists in table "+temp1[0]);
					return;
				}

				for(int i=0;i<resultData.size();i++){
					ArrayList<Integer> t = resultData.get(i);
					if(mark.get(i)==true && t.get(index) > max)
						max = t.get(index);
				}
				aggregate.put(s, max+"");
				aggregateFlag=1;
			}
			else if(s.startsWith("min")){
//				System.out.println("Inside Aggregate min");
				int min=Integer.MAX_VALUE;
				int index1=s.indexOf('(');
				int index2=s.lastIndexOf(')');
				int index = -1;
				int flag=0;
				
				String attribute =s.substring(index1+1, index2);
				attribute = attribute.trim();
				
				String temp1[]=null;
				if(resultAttributes.contains(attribute))
					index=resultAttributes.indexOf(attribute);
				else{
					
					for(String s1 : resultAttributesTable){
						if(attribute.startsWith(s1)){
							temp1 = attribute.split("\\.");
							if(!resultAttributesTable.contains(temp1[0])){
								System.out.println("Error: table "+temp1[0]+" doesnot exists");
								return;
							}
							for(String s2: resultAttributes){
								if(s2.equals(temp1[1]) && temp1[0].equals(s1)){
									index=resultAttributes.indexOf(s2);
									flag=1;
									break;
								}
							}
						}
						if(flag==1)
							break;
					}
				}
				if(index==-1){
					System.out.println("Error: column "+temp1[1]+" doesnot exists in table "+temp1[0]);
					return;
				}

				for(int i=0;i<resultData.size();i++){
					ArrayList<Integer> t = resultData.get(i);
					if(mark.get(i)==true && t.get(index) < min)
						min = t.get(index);
				}
				aggregate.put(s, min+"");
				aggregateFlag=1;
			}
			else if(s.startsWith("avg")){
//				System.out.println("Inside Aggregate avg");
				double avg=0;
				int index1=s.indexOf('(');
				int index2=s.lastIndexOf(')');
				int index = -1;
				int flag=0;
				
				String attribute =s.substring(index1+1, index2);
				attribute = attribute.trim();
				
				String temp1[]=null;
				if(resultAttributes.contains(attribute))
					index=resultAttributes.indexOf(attribute);
				else{
					
					for(String s1 : resultAttributesTable){
						if(attribute.startsWith(s1)){
							temp1 = attribute.split("\\.");
							if(!resultAttributesTable.contains(temp1[0])){
								System.out.println("Error: table "+temp1[0]+" doesnot exists");
								return;
							}
							for(String s2: resultAttributes){
								if(s2.equals(temp1[1]) && temp1[0].equals(s1)){
									index=resultAttributes.indexOf(s2);
									flag=1;
									break;
								}
							}
						}
						if(flag==1)
							break;
					}
				}
				if(index==-1){
					System.out.println("Error: column "+temp1[1]+" doesnot exists in table "+temp1[0]);
					return;
				}
				double sum=0;
				int cnt=0;
				for(int i=0;i<resultData.size();i++){
					ArrayList<Integer> t = resultData.get(i);
					if(mark.get(i)==true){
						sum+=t.get(index);
						cnt++;
					}
				}
				avg =(double)sum / cnt;
				aggregate.put(s, avg+"");
				aggregateFlag=1;
			}
			else if(s.startsWith("sum")){
//				System.out.println("Inside Aggregate sum");
				int sum=0;
				int index1=s.indexOf('(');
				int index2=s.lastIndexOf(')');
				int index = -1;
				int flag=0;
				
				String attribute =s.substring(index1+1, index2);
				attribute = attribute.trim();
				
				String temp1[]=null;
				if(resultAttributes.contains(attribute))
					index=resultAttributes.indexOf(attribute);
				else{
					
					for(String s1 : resultAttributesTable){
						if(attribute.startsWith(s1)){
							temp1 = attribute.split("\\.");
							if(!resultAttributesTable.contains(temp1[0])){
								System.out.println("Error: table "+temp1[0]+" doesnot exists");
								return;
							}
							for(String s2: resultAttributes){
								if(s2.equals(temp1[1]) && temp1[0].equals(s1)){
									index=resultAttributes.indexOf(s2);
									flag=1;
									break;
								}
							}
						}
						if(flag==1)
							break;
					}
				}
				if(index==-1){
					System.out.println("Error: column "+temp1[1]+" doesnot exists in table "+temp1[0]);
					return;
				}
				int sum1=0;
				for(int i=0;i<resultData.size();i++){
					ArrayList<Integer> t = resultData.get(i);
					if(mark.get(i)==true)
						sum1+=t.get(index);
				}
				aggregate.put(s, sum1+"");
				aggregateFlag=1;
			}
		}
		
		int flag3=0;
		if(aggregateFlag==1){
			for(String s: aggregate.keySet())
				System.out.print(s+"\t");
			System.out.println();
			for(String s: aggregate.keySet())
				System.out.print("===========");
			System.out.println();
			for(String s : aggregate.values())
				System.out.print(s+"\t");
			System.out.println();
			
		}else{
//			System.out.println("Inside projection");
			
			ArrayList<Integer> projectIndexes = new ArrayList<Integer>();
			for(String s : columns){
//				System.out.println("Current column : "+s);
				if(s.equals("*")){
//					System.out.println("Inside select *");
					for (String s1 : resultAttributes)
						System.out.print(s1 + "\t");
					System.out.println();
					for (int k=0;k<resultAttributes.size();k++)
						System.out.print("========");
					System.out.println();

					for(int i = 0; i < resultData.size();i++){
						if(mark.get(i)==true){
							ArrayList<Integer> temp1 = resultData.get(i);
							for(int k : temp1)
								System.out.print(k + "\t");
							System.out.println();
						}
					}
					flag3=1;	
					break;
				}// end of select *
				else{
//					System.out.println("Inside not select *");
					String temp[];
					if(s.contains(".")){
						temp = s.split("\\.");
						for(int i = 0; i<resultAttributes.size();i++){
							if(resultAttributes.get(i).equals(temp[1]) && resultAttributesTable.get(i).equals(temp[0])){
								int index = i;
//								System.out.println("Column : "+s+" index : "+index);
								resultAttributes.set(index,s);
								projectIndexes.add(index);
								break;
							}	
						}
					}else{
						int cnt = Collections.frequency(resultAttributes, s);
						if(cnt>1){
							System.out.println("Error: Specify table name along with column name");
							return;
						}
						int index = resultAttributes.indexOf(s);
//						System.out.println("Column : "+s+" index : "+index);
						projectIndexes.add(index);
					}
				}// end of a column
			}// end of loop of project columns
			
//			System.out.println("Projection on indexes : "+projectIndexes);
			
			if(flag3!=1){ // checking if projections or select *
				
				for (int x=0;x<resultAttributes.size();x++){
					if(projectIndexes.contains(x))
						System.out.print(resultAttributes.get(x) + "\t");
				}
				System.out.println();

				for (int k=0;k<resultAttributes.size();k++){
					if(projectIndexes.contains(k))
						System.out.print("========");
				}
				System.out.println();
				ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
				for(int i = 0; i < resultData.size();i++){
					if(mark.get(i)==true){
						
						ArrayList<Integer> temp1 = resultData.get(i);
						ArrayList<Integer> temp2 = new ArrayList<Integer>();
						for(int k=0;k<temp1.size();k++){
							if(projectIndexes.contains(k)){
								temp2.add(temp1.get(k));
							}
						}
						int flag=0;
						for(ArrayList<Integer> x : result){
							if(x.equals(temp2)){
								flag=1;
								break;
							}
						}
						if(flag==0)
							result.add(temp2);
					}
				}
				for(ArrayList<Integer> l : result){
					for(int i : l)
						System.out.print(i + "\t");
					System.out.println();
				}		
			}// End of printing projections
		}// End of if not Aggregate function
	}
}