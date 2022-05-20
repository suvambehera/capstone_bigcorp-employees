# bigcorp-employees

## Project Objective
Create a date engineering solution to a HR employee data by formulating a end-to-end pipeline utilising Big data and Machine Learning technolgy stacks to gain insights using exploratory data analysis and to build ML clasffication models.


## Data
The data acquired is from a major corporation's employees database over a period of 15 years. The data is spread across 6 .csv files with varying degree of cardinal relationship between them. The goal is extraction, transformation an loading of data to gain valuable insights as well as to predict the likeliood of employees tenure in the organisation.


### Tables
The physical model is of this denormalised database is a Star Schema with a key-dimension tables.
- [employees.csv](https://github.com/suvambehera/capstone_bigcorp-employees/files/8735212/employees.csv) which has the data of former and current every employee.
- [departments.csv](https://github.com/suvambehera/capstone_bigcorp-employees/files/8735210/departments.csv) which has the data about the various departments within the organisation.
- [dept_manager.csv](https://github.com/suvambehera/capstone_bigcorp-employees/files/8735214/dept_manager.csv) about the manager and their working department.
- [dept_emp.csv](https://github.com/suvambehera/capstone_bigcorp-employees/files/8735216/dept_emp.csv) consists of information of all employees within any particular department.
- [salaries.csv](https://github.com/suvambehera/capstone_bigcorp-employees/files/8735224/salaries.csv) has the salary information about each employee.
- [titles.csv](https://github.com/suvambehera/capstone_bigcorp-employees/files/8735231/titles.csv) stores the working designation of the employees.


### Data Description

- Titles (titles.csv): \
   title id-Unique id of type of employee (designation id) - Character - Not Null \
   title-Designation Character - Not Null

- Employees (employees.csv): \
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

- Salaries (salaries.csv): \
   emp no- Employee id- Integer - Not Null Salary \
   Employee's Salary-Integer - Not Null

- Departments (departments.csv): \
   dept no-Unique id for each department - character - Not Null \
   dept_name-Department Name-Character - Not Null

- Department Managers (dept_manager.csv): \
   dept no - Unique id for each department-character - Not Null \
   emp_no-Employee number (head of the department )- Integer - Not Null

- Department Employees (dept_emp.csv): \
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

### Database creation: 
   1) Connecting to Linux terminal to access MySQL server for database creation.
         ```
           mysql -u username -p 
         ```
   2) Schema definition - In the database create 6 table corresponding to the files, with proper schema.
    
       ``` 
         create database anabig114211; 
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
           emp_no int NOT NULL); 
           
       ```
    
   3) Upload the data into the HDFS FTP
    
       ``` /home/anabig114211/capstonelv1_empdata ```
    
   4) Data Ingestion - Load the data into the table using appropriate delimiter.
    
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
    1) Select a compressed file format (AVRO) for the data to be transferred.
    2) Use Sqoop command to transfer all table data to a specified location into HDFS directory.
    
        ``` sqoop import-all-tables  --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114211
           --username anabig114211 --password Bigdata123 --compression-codec=snappy --as-avrodatafile 
           --warehouse-dir=/user/anabig114211/hive/warehouse/jes --m 1 --driver com.mysql.jdbc.Driver ```
      
    3) In Linux shell, transfer the .avsc schema file to another specified HDFS directory.
      ```
           <!-- check if all files were imported successfully from the Sqoop tranfer -->
           <!-- avsc schema files in linux home -->
           ls -l /home/anabig114211/*.avsc

           <!-- create a directory dedicated to the project in HDFS -->
           hdfs dfs -mkdir /user/anabig114211/caplvl1_avsc

           <!-- transfer all the avro schema data files from the Linux to HDFS using the -put command -->
           hdfs dfs -put /home/anabig114211/titles.avsc /user/anabig114211/caplvl1_avsc/titles.avsc

           hdfs dfs -put /home/anabig114211/salaries.avsc /user/anabig114211/caplvl1_avsc/salaries.avsc

           hdfs dfs -put /home/anabig114211/despartments.avsc /user/anabig114211/caplvl1_avsc/departments.avsc

           hdfs dfs -put /home/anabig114211/dept_emp.avsc /user/anabig114211/caplvl1_avsc/dept_emp.avsc

           hdfs dfs -put /home/anabig114211/dept_manager.avsc /user/anabig114211/caplvl1_avsc/dept_manager.avsc

           hdfs dfs -put /home/anabig114211/employees.avsc /user/anabig114211/caplvl1_avsc.employees.avsc

           hdfs dfs -ls /user/anabig114211/caplvl1_avsc
       ```

    
3. Hive database and table creation
    1) Create a database and external table for the table data imported.
    2) Data Ingestion - Load the data into external table using appropriate path and SerDes.
    
         ```
             --<!-- Hive table creation using the avro schema files -->
            CREATE EXTERNAL TABLE departments
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            location "/user/anabig114238/hive/warehouse/departments"
            TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/departments.avsc');

            CREATE EXTERNAL TABLE employees
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            location "/user/anabig114238/hive/warehouse/employees"
            TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/employees.avsc');

            CREATE EXTERNAL TABLE salaries
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            location "/user/anabig114238/hive/warehouse/salaries"
            TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/salaries.avsc');

            CREATE EXTERNAL TABLE titles
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            location "/user/anabig114238/hive/warehouse/titles"
            TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/titles.avsc');

            CREATE EXTERNAL TABLE dept_emp
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            location "/user/anabig114238/hive/warehouse/dept_emp"
            TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/dept_emp.avsc');

            CREATE EXTERNAL TABLE dept_manager
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
            location "/user/anabig114238/hive/warehouse/dept_manager"
            TBLPROPERTIES ('avro.schema.url'='/user/anabig114211/caplvl1_avsc/dept_manager.avsc');

            --<!-- check the database for tables -->
            show tables;

            --<!-- check the talbes for data -->
            select * from departments;

            select * from employees;

            select * from titles;

            select * from dept_emp;

            select * from dept_manager;

            select * from salaries;
         ```
   
    3) Create views from the tables for specified purposes.
       ```
            CREATE VIEW employeesorg AS
                SELECT emp_no,                                                      
            emp_title,                                                                       
            birth_date,                                                                       
            first_name,                                                                        
            last_name,                                                                        
            sex,                                                                            
            hire_date,                                                                            
            no_of_projects,                                                                          
            last_performance_rating,                                                                         
            last_date, left_org
            FROM(
               SELECT emp_no,                                                      
            emp_title,                                                                       
            birth_date,                                                                       
            first_name,                                                                        
            last_name,                                                                        
            sex,                                                                            
            hire_date,                                                                            
            no_of_projects,                                                                          
            last_performance_rating,                                                                         
            last_date,
                   CASE WHEN LENGTH(last_date) > 8 THEN '1' 
                       WHEN LENGTH(last_date) > 0 THEN '0'
                   END AS left_org
            FROM employees)t1;

            --<!-- Salary bins view -->
            CREATE view BINS as
            SELECT
              CASE 
                   WHEN s.salary >= 40000 and s.salary < 50000 THEN '40k-50k'
                   WHEN s.salary >= 50000 and s.salary <60000 THEN '50k-60k'
                   WHEN s.salary >= 60000 and s.salary < 70000 THEN '60k-70k'
                   WHEN s.salary >= 70000 and s.salary < 80000 THEN '70k-80k'
                   WHEN s.salary >= 80000 and s.salary < 90000 THEN '80k-90k'
                   WHEN s.salary >= 90000 and s.salary < 100000 THEN '90k-100k'
                   WHEN s.salary >= 100000 and s.salary < 110000 THEN '100k-110k'
                   WHEN s.salary >= 110000 and s.salary < 120000 THEN '110k-120k'
                   WHEN s.salary >= 120000 and s.salary < 130000 THEN '120k-130k'
                   ELSE 'NA'
                   END AS Bins
            FROM employeesorg e
            JOIN salaries s
            ON s.emp_no = e.emp_no;

            --<!-- View for Tenure distribution -->
            CREATE VIEW employees_tenure AS
            SELECT
               emp_no,
               first_name,
               last_name,
               hire_date,
               CAST(SUBSTR(hire_date, -4,4) AS INT) AS hire_year,
               left_org,
               last_date,
               CAST(SUBSTR(last_date, -5, 4) AS INT) AS left_year
            FROM employeesorg;
         ```
         
4. Impala and SparkSQL EDA
    1) Invalidate Metadata to reload the fresh metadata for Impala query.
    2) Do EDA on te business problem asked.
    

Q1. A list of employee number, last name, first name, sex, and salary for each employee.

      SELECT s.emp_no, e.last_name, e.first_name, e.sex, s.salary 
      FROM employeesorg e INNER JOIN salaries s 
          ON e.emp_no = s.emp_no; ```
          
       ![image](https://user-images.githubusercontent.com/86786263/169455533-bcdaf314-be4f-4a94-b81b-5c36d6e10ccd.png)
       
 Q2. First name, last name, and hire date for employees who were hired in 1986

      SELECT first_name, last_name, hire_date
      FROM employeesorg
      WHERE hire_date LIKE '%1986';


Q3. List showing Manager of each department  with dept number, dept name, manager's emp number, last name and first name. 

      SELECT d.dept_no, d.dept_name, dm.emp_no, e.last_name, e.first_name
      FROM departments d INNER JOIN dept_manager dm
          ON d.dept_no = dm.dept_no LEFT JOIN employeesorg e
          ON e.emp_no = dm.emp_no;

    
Q4. List dept of each employee with emp number, last name, first name, dept name.

      SELECT e.emp_no, e.last_name, e.first_name, d.dept_name
      FROM employeesorg e INNER JOIN dept_emp de
          ON e.emp_no = de.emp_no INNER JOIN departments d
          ON de.dept_no = d.dept_no;

    
Q5. List of employees first name, last name, sex with first name 'Hercules' and last name begin with 'B'.

      SELECT first_name, last_name, sex
      FROM employeesorg
      WHERE first_name LIKE 'Hercules' AND
          last_name LIKE 'B%';

    
Q6. List of all employees in Sales departments with their emp number, last name, first name, dept name.

      SELECT de.emp_no, e.last_name, e.first_name, d.dept_name
      FROM departments d INNER JOIN dept_emp de
          ON d.dept_no = de.dept_no INNER JOIN employeesorg e
          ON de.emp_no = e.emp_no
      WHERE d.dept_name LIKE '%Sales%';


Q7. List of all employees in Sales and Development departments with their emp number, last name, first name, dept name.

      SELECT de.emp_no, e.last_name, e.first_name, d.dept_name
      FROM departments d INNER JOIN dept_emp de
          ON d.dept_no = de.dept_no INNER JOIN employeesorg e
          ON de.emp_no = e.emp_no
      WHERE d.dept_name LIKE '%Sales%' or d.dept_name LIKE '%development%';


Q8. List employee count with same last name

      SELECT last_name, COUNT(last_name) as surname_count
      FROM employeesorg
      GROUP BY last_name
      ORDER BY COUNT(last_name) DESC;


Q9. Salary distribution using histogram

      SELECT bins, COUNT(*)
      FROM bins
      GROUP BY bins;

![image](https://user-images.githubusercontent.com/86786263/169456015-733935ee-b469-4b5a-8f53-49ca28a71d3d.png)


Q10.Average salary per designation using Bar graph

      SELECT t.title, AVG(s.salary) as avg_salary
      FROM employeesorg e INNER JOIN salaries s 
          ON e.emp_no = s.emp_no INNER JOIN titles t  
          ON t.title_id = e.emp_title
      GROUP BY t.title;

![image](https://user-images.githubusercontent.com/86786263/169456098-78812b7a-dc0f-46c6-853f-73cf3867a588.png)

Q11. Calculate employee tenure and tenure distribution among employees.

      SELECT COUNT(emp_no) AS emp_count,
          CASE    
              WHEN left_year IS NULL THEN (2013-hire_year)
              ELSE (left_year - hire_year)
              END AS tenure
      FROM employees_tenure
      GROUP BY CASE    
              WHEN left_year IS NULL THEN (2013-hire_year)
              ELSE (left_year - hire_year)
              END
      ORDER BY tenure;

![image](https://user-images.githubusercontent.com/86786263/169456244-7e2e48bc-691b-4999-9169-78a0c6292a71.png)


Q12. Net expenditure for each dept

      SELECT d.dept_name, SUM(s.salary)
      FROM departments d LEFT JOIN dept_emp de 
          ON d.dept_no = de.dept_no INNER JOIN employeesorg e 
          ON de.emp_no = e.emp_no INNER JOIN salaries s 
          ON e.emp_no = s.emp_no
      GROUP BY d.dept_name;


Q13. Sex ratio each department

      SELECT d.dept_name, SUM(CASE
                          WHEN e.sex = 'M' THEN 1 ELSE 0 END) AS Male_Count, SUM(CASE
                                      WHEN e.sex = 'F' THEN 1 ELSE 0 END) AS Female_Count, 
              COUNT(IF(e.sex = 'M', 1, NULL))/COUNT(IF(e.sex = 'F', 1, NULL)) as ratio
      FROM employeesorg e LEFT JOIN dept_emp de 
          ON e.emp_no = de.emp_no INNER JOIN departments d 
          ON d.dept_no = de.dept_no
          GROUP BY d.dept_name;
    
    
Q14. Highest paid employee first and last name

      SELECT d.dept_name, e.first_name, e.last_name, s.salary
      FROM departments d LEFT JOIN dept_emp de 
          ON d.dept_no = de.dept_no INNER JOIN employeesorg e 
          ON de.emp_no = e.emp_no INNER JOIN salaries s 
          ON e.emp_no = s.emp_no
      ORDER BY s.salary
      LIMIT 1;


Q15. Number of employee at each designation

      SELECT t.title, COUNT(*) AS total_employees
      FROM employeesorg e INNER JOIN titles t 
          ON e.emp_title = t.title_id
      GROUP BY t.title;


Q16. Performance rating distribution

      SELECT e.last_performance_rating, d.dept_name, COUNT(*) AS total_emp
      FROM departments d LEFT JOIN dept_emp de 
          ON d.dept_no = de.dept_no INNER JOIN employeesorg e 
          ON de.emp_no = e.emp_no
      GROUP BY e.last_performance_rating, d.dept_name
      ORDER BY total_emp DESC;

    c. Open Jupyter notebook and create Spark Session instance.
    d. Use SparkSQL to perform EDA of the same business problems.
    e. Perform proper visualisation.
    
Please refer to [SparkSQL EDA](https://github.com/suvambehera/capstone_bigcorp-employees/blob/main/SparkSQL/Capstone1_SparkSQL.ipynb)
    
5. SparkML Model building
    a. Import all the required libraries in Jupyter Labs
    b. Data preparation to clean out duplicate and null values.
    c. Proper formatting of table fields.
    d. Create categorical and continous features.
    e. Do Encoding on the categorical data and transform the data.
    f. Slit the dataset into train and test set.
    g. Perform Logistic Regression and Random Forest Classifier.
    h. Compare the result.
    
Please refer to [SparkML Model](https://github.com/suvambehera/capstone_bigcorp-employees/blob/main/SparkML/capstone1_SparkML.ipynb)
    

