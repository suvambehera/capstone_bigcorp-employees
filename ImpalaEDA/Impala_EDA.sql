   Q1. A list of employee number, last name, first name, sex, and salary for each employee.

         SELECT s.emp_no, e.last_name, e.first_name, e.sex, s.salary 
         FROM employeesorg e INNER JOIN salaries s 
             ON e.emp_no = s.emp_no;

       

    Q2. First name, last name, and hire date for employees who were hired in 1986

         SELECT first_name, last_name, hire_date
         FROM employeesorg
         WHERE hire_date LIKE '%1986';


   Q3. List showing Manager of each department  with dept number, dept name, managers emp number, last name and first name. 

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

 


   Q10.Average salary per designation using Bar graph

         SELECT t.title, AVG(s.salary) as avg_salary
         FROM employeesorg e INNER JOIN salaries s 
             ON e.emp_no = s.emp_no INNER JOIN titles t  
             ON t.title_id = e.emp_title
         GROUP BY t.title;


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

  
