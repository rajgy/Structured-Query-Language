# Import necessary libraries
from pyspark import SparkConf, SparkContext  # Spark configuration and context
from pyspark.sql import SparkSession  # Spark session for DataFrame operations
import os  # For setting environment variables
import sys  # For accessing system-specific parameters

# Step 1: Set up environment variables for PySpark
python_path = sys.executable  # Get the current Python interpreter path
os.environ['PYSPARK_PYTHON'] = python_path  # Tell PySpark to use this Python interpreter
os.environ['HADOOP_HOME'] = "hadoop"  # Set Hadoop home directory
os.environ['JAVA_HOME'] = r'C:\Users\ghimi\.jdks\corretto-1.8.0_442'  # Set Java home directory

# Step 2: Configure Spark
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("pyspark") \
    .getOrCreate()

# Print a message to indicate the program has started
print("STARTED=============")

#🔴 SQL PRE REQUISITE CODE

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()



data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()




data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()



# Register DataFrame as a temporary view
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView(("df1"))
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

#Select require column from dataframe
spark.sql("SELECT id, tdate from df").show()

#select the category= Exercise form df
spark.sql("select * from df where category ='Exercise'").show()

#select the category = 'Exercise' and spendby ='cass' using where operator from df
spark.sql("select * from df where category = 'Exercise' and spendby = 'cash'").show()

#Multi value filter --select category = Exercise and Gymnastics from df
spark.sql("select * from df where category in ('Exercise', 'Gymnastics')").show()

#Like operetor --- slect the product contains Gymnastics
spark.sql("select * from df where product like '%Gymnastics%'").show()

#Not equal filter ---- select category not equals to Exercise
spark.sql("select*from df where category != 'Exercise'").show()

#Not equal filter for multi value  ---- select category not equals to Exercise and Gymnastics
spark.sql("select*from df where category not in ('Exercise','Gymnastics')").show()

#Null filter ---select the null value of the filter
spark.sql("select * from df where product is null").show()

#max id operator----- select maximum id from df
spark.sql("select max(id) from df").show()


#Rows count
spark.sql("select count(1) from df").show()

#conditional statement
spark.sql("select *, case when spendby = 'cash' then 1 else 0 end as status from df").show()

#multiple conditional statement
spark.sql("select *, case when spendby = 'cash' then 1 when spendby ='paytm' then 2 else 0 end as status from df").show()


#Concat two columns
spark.sql("select id, category, concat(id,'-',category) as condata from df ").show()


#Concat multiple columns
spark.sql("select id, category,product, concat(id,'-',category,'-',product) as condata from df ").show()


#Concat multiple columns
spark.sql("select id, category,product, concat_ws('-',id,category,product) as condata from df ").show()

#Lower of category
spark.sql("select category, lower(category) as lower_category from df").show()

#upper of category
spark.sql("select category, upper(category) as lower_category from df").show()

#ceil operator ---rounding upto the upper value
spark.sql("select amount,ceil(amount) as ceil from df").show()

#round operator ---rounding upto the upper value
spark.sql("select amount,round(amount) as ROUND from df").show()

#Replace nulls---used coalesce function
spark.sql("select product, coalesce(product,'NA') as nullrep from df").show()

#Trim and space
spark.sql("select trim(product) as trimproduct from df").show()

# distinct------find out the unique value
spark.sql("select distinct category from df").show()

# distinct on multiple columns------find out the unique value
spark.sql("select distinct category,spendby from df").show()

#substing ----need limitation of character from the columns
spark.sql("select substring(product,1,10) as sub from df").show()

#split operation
spark.sql("select product, split(product,'')[0] as split from df").show()

#Union All-- clubing/combining of two dataframes
spark.sql("select * from df union all select * from df1 as unionData").show()

#Union-- clubing/combining of two dataframes
spark.sql("select * from df union select * from df1 as unionData").show()

#Aggregate SUM for single column---EXAMPLE---> TOTAL AMOUT COLUMN FOR EACH CATEGORY---
spark.sql("select category, sum(amount) as sum from df group by category").show()

#Aggregate SUM for multiple column---EXAMPLE---> TOTAL AMOUT COLUMN FOR EACH CATEGORY---
spark.sql("select category,spendby, sum(amount) as sum from df group by category,spendby").show()

#Count --Aggregate SUM for single column---EXAMPLE---> TOTAL AMOUT COLUMN FOR EACH CATEGORY---
spark.sql("select category, sum(amount) as sum, count(amount) as cnt from df group by category").show()

#Aggregate MAX for single column---EXAMPLE---> TOTAL AMOUT COLUMN FOR EACH CATEGORY---
spark.sql("select category, max(amount) as sum from df group by category order by category").show()

#Window Row operation ---Section of data--separate operation for each section is known as windowing
spark.sql("select category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()

#Window Dense_rank Number
spark.sql("select category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()

#Window rank Number
spark.sql("select category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()

#Window Lead function
spark.sql("select category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()

#Window lag  function
spark.sql("select category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()

#Having function
spark.sql("select category, count(category) as count from df group by category having count(category)>1").show()

#Inner Join
spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()


#Left Join
spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()

#Right Join
spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()


#Full Join
spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()

#left anti Join
spark.sql("select a.id,a.name from cust a left anti join  prod b on a.id=b.id").show()

#Date format
spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()

#Sub query
spark.sql("""select sum(amount) as total , con_date from(select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df) group by con_date """).show()






