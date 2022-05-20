#!/usr/bin/env python
# coding: utf-8

# In[41]:


from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("suvam_capstone1")        .config("hive.metastore.uris","thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083")        .enableHiveSupport().getOrCreate())


# In[42]:


spark


# ### 1. List employee number, first name, last name, sex, salary for each employee

# In[43]:


spark.sql("select e.emp_no, e.last_name, e.first_name, e.sex, s.salary    from suvamalabs.employeesorg e inner join suvamalabs.salaries s    on e.emp_no = s.emp_no").show()


# ### 2. First name, last name, and hire date for employees who were hired in 1986

# In[44]:


spark.sql("select first_name, last_name, hire_date    from suvamalabs.employeesorg    where hire_date LIKE '%1986'").show()


# ###  3. List showing Manager of each department  with dept number, dept name, manager's emp number, last name and first name. 

# In[45]:


spark.sql("select d.dept_no, d.dept_name, dm.emp_no, e.last_name, e.first_name    from suvamalabs.departments d inner join suvamalabs.dept_manager dm    on d.dept_no = dm.dept_no left join suvamalabs.employeesorg e    on e.emp_no = dm.emp_no").show()


# ### 4. List dept of each employee with emp number, last name, first name, dept name.

# In[46]:


spark.sql("select e.emp_no, e.last_name, e.first_name, d.dept_name    from suvamalabs.employeesorg e inner join suvamalabs.dept_emp de    on e.emp_no = de.emp_no inner join suvamalabs.departments d    on de.dept_no = d.dept_no").show()


# ### 5. List of employees first name, last name, sex with first name 'Hercules' and last name begin with 'B'.

# In[47]:


spark.sql("select first_name, last_name, sex    from suvamalabs.employeesorg    where first_name LIKE 'Hercules' and        last_name LIKE 'B%'").show()


# ### 6. List of all employees in Sales departments with their emp number, last name, first name, dept name.

# In[48]:


spark.sql("""select de.emp_no, e.last_name, e.first_name, d.dept_name    from suvamalabs.departments d inner join suvamalabs.dept_emp de    on d.dept_no = de.dept_no inner join suvamalabs.employeesorg e    on de.emp_no = e.emp_no    where d.dept_name LIKE '%Sales%'""").show()


# ### 7. List of all employees in Sales and Development departments with their emp number, last name, first name, dept name.

# In[49]:


spark.sql("""select de.emp_no, e.last_name, e.first_name, d.dept_name    from suvamalabs.departments d inner join suvamalabs.dept_emp de    on d.dept_no = de.dept_no inner join suvamalabs.employeesorg e    on de.emp_no = e.emp_no    where d.dept_name like '%Sales%' or d.dept_name like '%development%'""").show()


# ### 8. List employee count with same last name

# In[50]:


spark.sql("select last_name, count(last_name) as surname_count    from suvamalabs.employeesorg    group by last_name    order by count(last_name) desc").show()


# ### 9. Histogram of Salary distribution

# In[51]:


import matplotlib.pyplot as plt
import pandas as pd


# In[52]:


salaries = spark.sql("select * from suvamalabs.salaries")


# In[53]:


salary = salaries.toPandas()


# In[54]:


salary


# In[55]:


plt.rcParams['figure.figsize']=(10,7)
plt.hist(salary['salary'],bins=9,color='xkcd:lavender',alpha=1,edgecolor='black')
plt.title('Salary distribution',fontsize=20,pad=40)
plt.xlabel('Salaries',fontsize=16,color='black',labelpad=20)
plt.ylabel('Frequency Count',fontsize=16,color='black',labelpad=20)
plt.xlim(36000,130000)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.show()


# In[56]:


plt.savefig('employee_salary_distribution.png')

plt.show()


# In[57]:


spark.sql("select bins, count(*)    from suvamalabs.bins    group by bins").show()


# ### 10. Average salary per designation usng Bar Chart 

# In[58]:


Avg_salary = spark.sql("select t.title, avg(s.salary) as avg_salary    from suvamalabs.employeesorg e inner join suvamalabs.titles t    on e.emp_title = t.title_id inner join suvamalabs.salaries s    on e.emp_no = s.emp_no group by t.title")


# In[59]:


Avg_Sal = Avg_salary.toPandas()


# In[60]:


Avg_Sal


# In[61]:


plt.rcParams['figure.figsize']=(10,7)
plt.bar(Avg_Sal['title'], Avg_Sal['avg_salary'])
plt.xlabel("Title")
plt.ylabel("Average Salary")
plt.title("Average Salary per Designation")

plt.show()


# ### 11. Employees tenure

# In[62]:


emp_ten = spark.sql("select count(emp_no) as total_emp,case                   when left_year is null then (2013-hire_year)                else (left_year - hire_year)                end as tenure            from suvamalabs.employees_tenure            group by case                    when left_year is null then (2013-hire_year)                else (left_year - hire_year)                end            order by tenure")


# In[63]:


emp_tenure = emp_ten.toPandas()


# In[64]:


emp_tenure


# In[65]:


plt.rcParams['figure.figsize']=(15,9)
plt.bar(emp_tenure['tenure'], emp_tenure['total_emp'])
plt.xlabel("Tenure in years")
plt.ylabel("Total employees")
plt.title("Employee tenure distribution")

plt.show()


# ### 12. Net expenditure each department

# In[69]:


dept_exp = spark.sql("SELECT d.dept_name, SUM(s.salary) AS total            FROM suvamalabs.departments d LEFT JOIN suvamalabs.dept_emp de                 ON d.dept_no = de.dept_no INNER JOIN suvamalabs.employeesorg e                 ON de.emp_no = e.emp_no INNER JOIN suvamalabs.salaries s                 ON e.emp_no = s.emp_no GROUP BY d.dept_name")


# In[71]:


dept_exp = dept_exp.toPandas()


# In[72]:


dept_exp


# In[74]:


plt.rcParams['figure.figsize']=(15,9)
plt.bar(dept_exp['dept_name'], dept_exp['total'])
plt.xlabel("Department")
plt.ylabel("Expenses")
plt.title("Net department expenditure")

plt.show()


# ### 13. Sex ratio department wise

# In[77]:


spark.sql("SELECT d.dept_name, SUM(CASE WHEN e.sex = 'M' THEN 1 ELSE 0 END) AS Male_Count,                  SUM(CASE WHEN e.sex = 'F' THEN 1 ELSE 0 END) AS Female_Count,         COUNT(IF(e.sex = 'M', 1, NULL))/COUNT(IF(e.sex = 'F', 1, NULL)) as ratio        FROM suvamalabs.employeesorg e LEFT JOIN suvamalabs.dept_emp de         ON e.emp_no = de.emp_no INNER JOIN suvamalabs.departments d         ON d.dept_no = de.dept_no GROUP BY d.dept_name").show()


# ### 14. Highest paid employee

# In[78]:


spark.sql("SELECT d.dept_name, e.first_name, e.last_name, s.salary            FROM suvamalabs.departments d LEFT JOIN suvamalabs.dept_emp de                 ON d.dept_no = de.dept_no INNER JOIN suvamalabs.employeesorg e                 ON de.emp_no = e.emp_no INNER JOIN suvamalabs.salaries s                 ON e.emp_no = s.emp_no            ORDER BY s.salary LIMIT 1").show()


# ### 15. Number of employees at each designation

# In[79]:


spark.sql("SELECT t.title, COUNT(*) AS total_employees        FROM suvamalabs.employeesorg e INNER JOIN suvamalabs.titles t             ON e.emp_title = t.title_id GROUP BY t.title").show()


# ### 16. Performance rating frequency

# In[82]:


spark.sql("SELECT e.last_performance_rating, d.dept_name, COUNT(*) AS total_emp            FROM suvamalabs.departments d LEFT JOIN suvamalabs.dept_emp de                 ON d.dept_no = de.dept_no INNER JOIN suvamalabs.employeesorg e                 ON de.emp_no = e.emp_no            GROUP BY e.last_performance_rating, d.dept_name ORDER BY total_emp DESC").show()


# In[ ]:




