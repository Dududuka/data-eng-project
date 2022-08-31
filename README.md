# ETL project

## The project involved extracting the available banking data, transforming the data and loading it to the database. After that a front-end console application was created for users to view or update the data, and several plots were created to highlight existing trends.

The project consists of the following stages:

1. Load data to the Credit Card System database. The data include details of customers, bank branches and credit card transactions. Data was extracted from JSON files, transformed according to the specifications and loaded to the MariaDB dababase. 
The tools used were Jupyter Notebook with Python 3, PySpark and SQL(MariaDB). One of the challenges was to make sure to set relations between the tables, for the database to be usable, which was not mentioned in the requirements.

2. At this stage, a front-end console application was created. The application allows to view and modify customer details as well as view transaction details and generate bills. The application was written in Python. SQL queries were run using mysql.connector.

3. This stage included creating plots to allow business analysts analyze and visualize the data. The tools used were Jupyter notebook with Python, in particular pandas and matplotlib libraries. 
Below are the five plots.

![Requirement 3.1 Plot](/Plots/req3_1.PNG "Req 3.1 Plot" )
![Requirement 3.2 Plot](/Plots/req3_2.PNG "Req 3.2 Plot" )
![Requirement 3.3 Plot](/Plots/req3_3.PNG "Req 3.3 Plot" )
![Requirement 3.4 Plot](/Plots/req3_4.PNG "Req 3.4 Plot" )
![Requirement 3.5 Plot](/Plots/req3_5.PNG "Req 3.5 Plot" )

4. At this stage, loan application data was added to the database. The data was retrieved using API endpoint and then loaded to  the SQL database using PySpark.

5. This stage included visualization of loan application data, such as approval rates by gender and income ranges or by property area. Similarly to the stage 3, it was implemented using Python pandas and matplotlib libraries. One of the challenges was to  create a multi-bar plot to illustrate approval rate for two gender and three income ranges.

To make the project accessible to the public, a github repository was created. The repository includes all the code, original data that was extracted, as well as screenshots of the plots.


