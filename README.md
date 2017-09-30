# SQL-Search-Engine
Implemented a mini sql engine which will run a subset of SQL Queries like Select, Join, Projection, Aggregate functions using command line interface. It uses Gudu SQL Parser to parse the SQL command.

Steps to run:
```
javac -cp .:gsp.jar:commons-lang3-3.5.jar SqlEngineMain.java
java -cp .:gsp.jar:commons-lang3-3.5.jar SqlEngineMain "Select * from apple;"
```
