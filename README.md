# bigcorp-employees
Creating a end-to-end pipeline utilising Big data and Machine Learning concepts to gain insights using exploratory data analysis and to build ML clasffication models.

## Data
The data acquired is from a major corporation's employees database over a period of 4 decades. The data is spread across six .csv files with varying degree of cardinal relationship between them. The goal is extract valuable insights as well as to predict the likeliood of employees tenure in the organisation.

### Tables
The physical model is of this denormalised database is a Star Schema with a key table [employees.csv](https://github.com/suvambehera/bigcorp-employees/files/8735212/employees.csv) which has the data of former and current every employee, and other dimensions tables like
[departments.csv](https://github.com/suvambehera/bigcorp-employees/files/8735210/departments.csv) which has the data about the various departments witin the organisation, [dept_manager.csv](https://github.com/suvambehera/bigcorp-employees/files/8735214/dept_manager.csv) about the manager and their working department, [dept_emp.csv](https://github.com/suvambehera/bigcorp-employees/files/8735216/dept_emp.csv) consists of information of all employees within any particuar department, [salaries.csv](https://github.com/suvambehera/bigcorp-employees/files/8735224/salaries.csv) has the salary information about each employee, and [titles.csv](https://github.com/suvambehera/bigcorp-employees/files/8735231/titles.csv) stores the working designation of the employees.

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

1. Linux - To interact with shell environment
2. MySQL - To create database
3. Sqoop - Transfer data from MySQL server to HDFS
4. HDFS - Store data
5. Hive - To create database and tables
6. Impala - To perform EDA
7. Jupyter Notebook - To create live code and visualisation
8. SparkSQL - To perform EDA
9. SparkML - Model building

## Data Model

   ![ERD](https://user-images.githubusercontent.com/86786263/169433695-059899b8-d42c-4d96-8bee-d92e69e97962.png)
   

## Architecture of Pipeline (Stages)

1. Database creation: 
    a. Connecting to Linux terminal to access MySQL server for database creation.
    b. In the database create 6 table corresponding to the files, with proper schema.
    c. Upload the data into the HDFS FTP
    d. Load the data into the table using appropriate delimiter.
   
2. Data and Schema transfer to HDFS and Hive
    a. Select a compressed file format (AVRO) for the data to be transferred.
    b. Use Sqoop command to transfer all table data to a specified location into HDFS directory.
    c. In Linux shell, transfer the .avsc schema file to another specified HDFS directory.
    
3. Hive database and table creation
    a. Create a database and external table for the table data imported.
    b. Load the data into external table using appropriate path and SerDes.
    c. Create views from the tables for specified purposes.

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
    
    
