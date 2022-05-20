mysql -u anabig114211 username -pBigdata123

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


/home/anabig114211/capstonelv1_empdata

mysql -u anabig114211 username -pBigdata123

load data local infile '/home/anabig114211/titles.csv' into table titles
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
   ignore 1 rows;



sqoop import-all-tables  --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114211
           --username anabig114211 --password Bigdata123 --compression-codec=snappy --as-avrodatafile 
           --warehouse-dir=/user/anabig114211/hive/warehouse/jes --m 1 --driver com.mysql.jdbc.Driver 


ls -l /home/anabig114211/*.avsc


hdfs dfs -mkdir /user/anabig114211/caplvl1_avsc


hdfs dfs -put /home/anabig114211/titles.avsc /user/anabig114211/caplvl1_avsc/titles.avsc

           hdfs dfs -put /home/anabig114211/salaries.avsc /user/anabig114211/caplvl1_avsc/salaries.avsc

           hdfs dfs -put /home/anabig114211/despartments.avsc /user/anabig114211/caplvl1_avsc/departments.avsc

           hdfs dfs -put /home/anabig114211/dept_emp.avsc /user/anabig114211/caplvl1_avsc/dept_emp.avsc

           hdfs dfs -put /home/anabig114211/dept_manager.avsc /user/anabig114211/caplvl1_avsc/dept_manager.avsc

           hdfs dfs -put /home/anabig114211/employees.avsc /user/anabig114211/caplvl1_avsc.employees.avsc

           hdfs dfs -ls /user/anabig114211/caplvl1_avsc


hive

use suvamalabs;

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


hive

use suvamalabs;

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