# bigcorp-employees

# Project Objective
Create a date engineering solution to a HR employee data by formulating a end-to-end pipeline utilising Big data and Machine Learning technolgy stacks to gain insights using exploratory data analysis and to build ML clasffication models.


## Data
The data acquired is from a major corporation's employees database over a period of 15 years. The data is spread across 6 .csv files with varying degree of cardinal relationship between them. The goal is extraction, transformation an loading of data to gain valuable insights as well as to predict the likeliood of employees tenure in the organisation.


### Tables
The physical model is of this denormalised database is a Star Schema with a key-dimension tables.
- [employees.csv](https://github.com/suvambehera/bigcorp-employees/files/8735212/employees.csv) which has the data of former and current every employee.
- [departments.csv](https://github.com/suvambehera/bigcorp-employees/files/8735210/departments.csv) which has the data about the various departments within the organisation.
- [dept_manager.csv](https://github.com/suvambehera/bigcorp-employees/files/8735214/dept_manager.csv) about the manager and their working department.
- [dept_emp.csv](https://github.com/suvambehera/bigcorp-employees/files/8735216/dept_emp.csv) consists of information of all employees within any particular department.
- [salaries.csv](https://github.com/suvambehera/bigcorp-employees/files/8735224/salaries.csv) has the salary information about each employee.
- [titles.csv](https://github.com/suvambehera/bigcorp-employees/files/8735231/titles.csv) stores the working designation of the employees.


### Data Description

a. Titles (titles.csv): \
title id-Unique id of type of employee (designation id) - Character - Not Null \
title-Designation Character - Not Null

b. Employees (employees.csv): \
emp_no-Employee Id - Integer - Not Null \
emp_titles_id-designation id-Not Null \
birth date-Date of Birth Date Time - Not Null. \
first name- First Name - Character Not Null \
last name - Last Name Character - Not Null \
sex-Gender-Character-Not Null \
hire date - Employee Hire date-Date Time -Not Null \
no of projects-Number of projects worked on-Integer- Not Null \
Last performance rating-Last year performance rating - Character - Not Null \
left-Employee left the organization - Boolean-Not Null \
Last date - Last date of employment (Exit Date) - Date Time

c. Salaries (salaries.csv): \
emp no- Employee id- Integer - Not Null Salary \
Employee's Salary-Integer - Not Null

d. Departments (departments.csv): \
dept no-Unique id for each department - character - Not Null \
dept_name-Department Name-Character - Not Null

e. Department Managers (dept_manager.csv): \
dept no - Unique id for each department-character - Not Null \
emp_no-Employee number (head of the department )- Integer - Not Null

f. Department Employees (dept_emp.csv): \
emp no-Employee id - Integer - Not Null \
dept no - Unique id for each department character - Not Null

## Technology stack

- Linux - To interact with shell environment
- MySQL - To create database
- Sqoop - Transfer data from MySQL server to HDFS
- HDFS - Store data
- Hive - To create database and tables
- Impala - To perform EDA
- Jupyter Notebook - To create live code and visualisation
- SparkSQL - To perform EDA
- SparkML - Model building


## Data Model

   ![ERD](https://user-images.githubusercontent.com/86786263/169433695-059899b8-d42c-4d96-8bee-d92e69e97962.png)
   

## Architecture of Pipeline (Stages)

1. Database creation: \
    a. Connecting to Linux terminal to access MySQL server for database creation.
    
    ``` mysql -u username -p ```
    
    b. Schema definition - In the database create 6 table corresponding to the files, with proper schema.
    
 ``` create database anabig114211; 
     use anabig114211; 
    
    
     create table titles(
     title_id varchar(10) PRIMARY KEY NOT NULL,
     title varchar(30) NOT NULL);

     create table employees(
     emp_no int PRIMARY KEY NOT NULL,
     emp_title_id varchar(10) NOT NULL,
     birth_date varchar(10) NOT NULL,
     first_name varchar(20) NOT NULL,
     last_name varchar(20) NOT NULL,
     sex varchar(5) NOT NULL,
     hire_date varchar(10) NOT NULL,
     no_of_projects int NOT NULL,
     last_performance_rating varchar(10) NOT NULL,
     left_org int NOT NULL,
     last_date varchar(10)); 

   <!-- make sure not to name any tables with MySQL reserved keywords -->

     create table salaries(
     emp_no int NOT NULL,
     salary bigint NOT NULL);

     create table departments(
     dept_no varchar(20) PRIMARY KEY NOT NULL ,
     dept_name varchar(30) NOT NULL);

     CREATE TABLE dept_emp(
     emp_no int NOT NULL,
     dept_no varchar(20) NOT NULL); 

     create table Department_Managers_jes(
     dept_no varchar(20) NOT NULL,
     emp_no int NOT NULL); ```
    
    c. Upload the data into the HDFS FTP
    
    ``` /home/anabig114211/capstonelv1_empdata ```
    
    d. Data Ingestion - Load the data into the table using appropriate delimiter.
    
  ``` load data local infile '/home/anabig114211/titles.csv' into table titles
      fields terminated by ','
      ignore 1 rows;

      load data local infile '/home/anabig114211/employees.csv' into table employees
      fields terminated by ','
      ignore 1 rows;

      load data local infile '/home/anabig114211/salaries.csv' into table salaries
      fields terminated by ','
      ignore 1 rows;

      load data local infile '/home/anabig114211/departments.csv' into table departments
      fields terminated by ','
      ignore 1 rows;

      load data local infile '/home/anabig114211/dept_emp.csv' into table dept_emp
      fields terminated by ','
      ignore 1 rows;

      load data local infile '/home/anabig114211/dept_manager.csv' into table dept_manager
      fields terminated by ','
      ignore 1 rows; ```
   
2. Data and Schema transfer to HDFS and Hive
    a. Select a compressed file format (AVRO) for the data to be transferred.
    b. Use Sqoop command to transfer all table data to a specified location into HDFS directory.
    
  ``` sqoop import-all-tables  --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114211
     --username anabig114211 --password Bigdata123 --compression-codec=snappy --as-avrodatafile 
     --warehouse-dir=/user/anabig114211/hive/warehouse/jes --m 1 --driver com.mysql.jdbc.Driver ```
      
    c. In Linux shell, transfer the .avsc schema file to another specified HDFS directory.
    
    - <!-- check if all files were imported successfully from the Sqoop tranfer -->
    - <!-- avsc schema files in linux home -->
    - ls -l /home/anabig114211/*.avsc

    - <!-- create a directory dedicated to the project in HDFS -->
    - hdfs dfs -mkdir /user/anabig114211/caplvl1_avsc

    - <!-- transfer all the avro schema data files from the Linux to HDFS using the -put command -->
    - hdfs dfs -put /home/anabig114211/titles.avsc /user/anabig114211/caplvl1_avsc/titles.avsc

    - hdfs dfs -put /home/anabig114211/salaries.avsc /user/anabig114211/caplvl1_avsc/salaries.avsc

    - hdfs dfs -put /home/anabig114211/despartments.avsc /user/anabig114211/caplvl1_avsc/departments.avsc

    - hdfs dfs -put /home/anabig114211/dept_emp.avsc /user/anabig114211/caplvl1_avsc/dept_emp.avsc

    - hdfs dfs -put /home/anabig114211/dept_manager.avsc /user/anabig114211/caplvl1_avsc/dept_manager.avsc

    - hdfs dfs -put /home/anabig114211/employees.avsc /user/anabig114211/caplvl1_avsc.employees.avsc

    - hdfs dfs -ls /user/anabig114211/caplvl1_avsc


    
3. Hive database and table creation
    a. Create a database and external table for the table data imported.
    b. Data Ingestion - Load the data into external table using appropriate path and SerDes.
    
    --<!-- Hive table creation using the avro schema files -->
   --CREATE EXTERNAL TABLE departments
   --ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   --STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   --OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   --location "/user/anabig114238/hive/warehouse/departments"
   --TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/departments.avsc');

   --CREATE EXTERNAL TABLE employees
   --ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   --STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   --OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   --location "/user/anabig114238/hive/warehouse/employees"
   --TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/employees.avsc');

   --CREATE EXTERNAL TABLE salaries
   --ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   --STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   --OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   --location "/user/anabig114238/hive/warehouse/salaries"
   --TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/salaries.avsc');

   --CREATE EXTERNAL TABLE titles
   --ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   --STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   --OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   --location "/user/anabig114238/hive/warehouse/titles"
   --TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/titles.avsc');

   --CREATE EXTERNAL TABLE dept_emp
   --ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   --STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   --OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   --location "/user/anabig114238/hive/warehouse/dept_emp"
   --TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/dept_emp.avsc');

   -- CREATE EXTERNAL TABLE dept_manager
   -- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   -- STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   -- OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   -- location "/user/anabig114238/hive/warehouse/dept_manager"
   -- TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/dept_manager.avsc');

   --<!-- check the database for tables -->
   -- show tables;

   --<!-- check the talbes for data -->
   -- select * from departments;

   -- select * from employees;

   -- select * from titles;

   -- select * from dept_emp;

   -- select * from dept_manager;

   -- select * from salaries;
   
    c. Create views from the tables for specified purposes.
    
   -- CREATE VIEW employeesorg AS
   --    SELECT emp_no,                                                      
   -- emp_title,                                                                       
   -- birth_date,                                                                       
   -- first_name,                                                                        
   -- last_name,                                                                        
   -- sex,                                                                            
   -- hire_date,                                                                            
   -- no_of_projects,                                                                          
   -- last_performance_rating,                                                                         
   -- last_date, left_org
   -- FROM(
   --    SELECT emp_no,                                                      
   -- emp_title,                                                                       
   -- birth_date,                                                                       
   -- first_name,                                                                        
   -- last_name,                                                                        
   -- sex,                                                                            
   -- hire_date,                                                                            
   -- no_of_projects,                                                                          
   -- last_performance_rating,                                                                         
   -- last_date,
   --        CASE WHEN LENGTH(last_date) > 8 THEN '1' 
   --            WHEN LENGTH(last_date) > 0 THEN '0'
   --        END AS left_org
   -- FROM employees)t1;
   
   --<!-- Salary bins view -->
   -- CREATE view BINS as
   -- SELECT
   --    CASE 
   --        WHEN s.salary >= 40000 and s.salary < 50000 THEN '40k-50k'
   --        WHEN s.salary >= 50000 and s.salary <60000 THEN '50k-60k'
   --        WHEN s.salary >= 60000 and s.salary < 70000 THEN '60k-70k'
   --        WHEN s.salary >= 70000 and s.salary < 80000 THEN '70k-80k'
   --        WHEN s.salary >= 80000 and s.salary < 90000 THEN '80k-90k'
   --        WHEN s.salary >= 90000 and s.salary < 100000 THEN '90k-100k'
   --        WHEN s.salary >= 100000 and s.salary < 110000 THEN '100k-110k'
   --        WHEN s.salary >= 110000 and s.salary < 120000 THEN '110k-120k'
   --        WHEN s.salary >= 120000 and s.salary < 130000 THEN '120k-130k'
   --        ELSE 'NA'
   --        END AS Bins
   -- FROM employeesorg e
   -- JOIN salaries s
   -- ON s.emp_no = e.emp_no;
   
   --<!-- View for Tenure distribution -->
   -- CREATE VIEW employees_tenure AS
   -- SELECT
   --    emp_no,
   --    first_name,
   --    last_name,
   --    hire_date,
   --    CAST(SUBSTR(hire_date, -4,4) AS INT) AS hire_year,
   --    left_org,
   --    last_date,
   --    CAST(SUBSTR(last_date, -5, 4) AS INT) AS left_year
   -- FROM employeesorg;

4. Impala and SparkSQL EDA
    a. Invalidate Metadata to reload the fresh metadata for Impala query.
    b. Do EDA on te business problem asked.
    c. Open Jupyter notebook and create Spark Session instance.
    d. Use SparkSQL to perform EDA of the same business problems.
    e. Perform proper visualisation.
    
5. SparkML Model building
    a. Import all the required libraries in Jupyter Labs
    b. Data preparation to clean out duplicate and null values.
    c. Proper formatting of table fields.
    d. Create categorical and continous features.
    e. Do Encoding on the categorical data and transform the data.
    f. Slit the dataset into train and test set.
    g. Perform Logistic Regression and Random Forest Classifier.
    h. Compare the result.
    

