This describes what we will do in this project.

A complete TPCH procedure contains 5 sub-procedures: Data Generate, Create Table, Load Data, Query, Pretty Report.
The following will describes how will we support these sub-procedures.

## Data Generate

This is a common procedure.

User provide the wanted table name, data size, file number, size per file,
then we can automatically generate corresponding CSV file to specified directories.

In this part, we will reuse the airlift-tpch version to generate the source file.

## Create Table

This is a customize procedure cause each DataBase/OLAP system may have different grammar to create the table.

### Way 1

For general usage, we will let the user provide the SQL statements for creating table.
We call this a `TableModel`.
A `TableModel` is mapped to a directory.
In this directory, there is a file called 'create_table_sqls.txt', each line represents a statement.

We will read the file and create the tables.

### Way 2

For some special databases or OLAP systems that we mainly targeted to,
we can provide special configuration such as TBLProperties,
and we can automatically generate corresponding SQL, then apply way1.
In this case, in the directly there is a file called 'create_table_sqls.json', each block represents a statement.


## Load Data

This is a customize procedure just like the 'Create Table' procedure.

We will handle this procedure just like the 'Create Table' procedure.

Note that we may need some preparation or postparation for this, such as uploading data to HDFS.

## Query

This is a common procedure.

User can provide the `QueryModel` for this.
A `QueryModel` is mapped to a directory.
In this directory, there is a file called 'query_sqls.json', each block represents a query.
The block describes the SQL to be executed, number of iteration for warm-up and execution.
There is also a file called 'driver.properties', which describes the driver url and other configurations.

For each `QueryModel`, we'll get a statistics. And we write that into a json file called 'statistics.json'.
We also provide corresponding runlog under that directory.

Note that we may need to deal with the NA results.

## Pretty Report

This is a common procedure.

If we want to compare the query performance for multiple query models,
we ca specify multiple query models. We will look into these folders and retrieve the 'statistics.json' files,
then  we will compare them and give the comparison result.
We can use the fist querymodel as the base,
then calculate the performance gained or loss for other queries.

Currently we only give a table format for this in text, later we can generate pretty excel file.

Note that we may need to deal with the NA results.