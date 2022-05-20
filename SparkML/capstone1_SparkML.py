#!/usr/bin/env python
# coding: utf-8

# In[514]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = (SparkSession.builder.appName("project").config("hive.metastore.uris","thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083").enableHiveSupport().getOrCreate())


# In[515]:


spark


# In[516]:


spark.sql('use suvamalabs').show()


# In[517]:


employees=spark.sql("select * from employeesorg")
departments=spark.sql("select * from departments")
dept_managers=spark.sql("select * from dept_manager")
dept_emp=spark.sql("select * from dept_emp")
salaries=spark.sql("select * from salaries")
titles=spark.sql("select * from titles")


# In[518]:


#cleaning name within quotes

departments = departments.withColumn('dept_name',regexp_replace('dept_name', '"', ''))


# In[519]:


#create pandas dataframe
employees_df = employees.toPandas()
departments_df = departments.toPandas()
dept_managers_df = dept_managers.toPandas()
dept_emp_df = dept_emp.toPandas()
salaries_df = salaries.toPandas()
titles_df = titles.toPandas()


# In[520]:


import pandas as pd


# In[521]:


emp_data = employees_df.merge(dept_emp_df, how='left', on='emp_no')


# In[522]:


emp_data_df1 = emp_data.merge(departments_df, how='left', on='dept_no')


# In[523]:


emp_data_df2 = emp_data_df1.merge(salaries_df, how='left', on='emp_no')


# In[524]:


emp_data_df3 = emp_data_df2.merge(titles_df, how='left', left_on = 'emp_title', right_on = 'title_id')


# In[525]:


emp_data_df3.info()


# In[526]:


emp_data_df3['last_date'] = emp_data_df3['last_date'].replace(to_replace = '\r', value = '', regex = True)


# In[527]:


emp_data_df3['last_performance_rating'] = emp_data_df3['last_performance_rating'].replace(to_replace = 'S',value = 4)
emp_data_df3['last_performance_rating'] = emp_data_df3['last_performance_rating'].replace(to_replace = 'A',value = 3)
emp_data_df3['last_performance_rating'] = emp_data_df3['last_performance_rating'].replace(to_replace = 'B',value = 2)
emp_data_df3['last_performance_rating'] = emp_data_df3['last_performance_rating'].replace(to_replace = 'C',value = 1)
emp_data_df3['last_performance_rating'] = emp_data_df3['last_performance_rating'].replace(to_replace = 'PIP',value = 0)


# In[528]:


emp_data_df3.head(10)


# In[529]:


emp_data_df3.isna().sum()


# In[530]:


import matplotlib.pyplot as plt
import seaborn as sn
get_ipython().run_line_magic('matplotlib', 'inline')


# In[531]:


import pandas as pd


# In[532]:


emp_data_df3.columns


# In[533]:


#create spark dataframe
emp_data_df4 = spark.createDataFrame(emp_data_df3)


# In[534]:


from pyspark.sql.functions import lit,col


# In[535]:


#Columns that will be used as features and their types

continuous_features = ['salary','no_of_projects']
                    
categorical_features = ['dept_no','title_id', 'sex']


# In[536]:


#Encoding all categorical features

from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, PolynomialExpansion, VectorIndexer


# In[537]:


#Create object of StringIndexer class and specify input and output column

SI_dept_no = StringIndexer(inputCol='dept_no',outputCol='dept_no_Idx5')
SI_sex = StringIndexer(inputCol='sex',outputCol='sex_Idx5')
SI_title_id = StringIndexer(inputCol='title_id',outputCol='title_id_Idx5')


# In[538]:


#Transform the data
emp_data_df4 = SI_dept_no.fit(emp_data_df4).transform(emp_data_df4)
emp_data_df4 = SI_sex.fit(emp_data_df4).transform(emp_data_df4)
emp_data_df4 = SI_title_id.fit(emp_data_df4).transform(emp_data_df4)


# In[539]:


# view the transformed data
emp_data_df4.select('dept_no', 'dept_no_Idx5', 'sex', 'sex_Idx5', 'title_id','title_id_Idx5').show(5)


# In[540]:


featureCols = continuous_features + ['dept_no_Idx5','sex_Idx5','title_id_Idx5'] 


# In[541]:


assembler = VectorAssembler( inputCols = featureCols, outputCol = "features")


# In[542]:


train_df1 = assembler.transform(emp_data_df4 )


# In[543]:


train_df1 = train_df1.withColumn('label', train_df1['left_org'].cast('integer'))


# In[544]:


#Split the dataset

train_df, test_df = train_df1.randomSplit( [0.7, 0.3], seed = 42 )


# ### Logistic Regression

# In[545]:


from pyspark.ml.classification import LogisticRegression


# In[546]:


#lr = LogisticRegression(maxIter=500, regParam=0.0)

lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)


# In[547]:


lm = lr.fit(train_df)


# In[548]:


#Make predictions on train data and evaluate

y_pred_train = lm.transform(train_df)


# In[549]:


y_pred_test = lm.transform( test_df )


# In[550]:


y_pred_test.select( 'features',  'label', 'prediction', 'left_org' ).show(10)


# In[552]:


from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()
print('Test Area under ROC', evaluator.evaluate(y_pred_test))


# ### Random Forest Classifier

# In[553]:


from pyspark.ml.classification import RandomForestClassifier
RF = RandomForestClassifier( featuresCol='features', labelCol='label')
RF_model = RF.fit(train_df)


# In[554]:


predictions = RF_model.transform(test_df)


# In[555]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[556]:


evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
accuracy = evaluator.evaluate(predictions)
accuracy

