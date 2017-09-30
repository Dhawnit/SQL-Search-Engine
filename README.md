# SQL-Search-Engine
Implemented a mini sql engine which will run a subset of SQL Queries like Select, Join, Projection, Aggregate functions using command line interface. It uses Gudu SQL Parser to parse the SQL command.

Here are the details of the project:

A. Dataset​:
	
	1.Csv files for tables. 
		a.If a file is :File1.csv then,the table name would be File1.
		b.There will be no tab­separation or space­separation 
	
	2.All the elements in files would be ​only INTEGERS​
	3.  A file named: metadata.txt​ would be given to you which will have the following structure for each table: 
	<begin_table>
	<table_name> 
	<attribute1> 
	.... 
	 
	<attributeN> 
	<end_table>

B. Type of Queries:

	1.Select all records​ : 
		Example:Select * from table_name;
	
	2.Aggregate functions:​Simple aggregate functions on a single column. Sum, average, max and min.
		Example: select max(col1) from table1;
		
	3.Project Columns​ from one or more tables :
		Example: Select col1, col2 from table_name; 
		
	4.Project with distinct from one table:
		Example:select distinct(col1) from table_name;
		
	5.Select with where from one or more tables:​ 
		Example:select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;

C. Error Handling:
  Almost all major types of errors are handled.
  
### Steps to run:
```
javac -cp .:gsp.jar:commons-lang3-3.5.jar SqlEngineMain.java
java -cp .:gsp.jar:commons-lang3-3.5.jar SqlEngineMain "Select * from apple;"
```
