from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Employee Data Cleaning Pipeline") \
    .getOrCreate()

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv("/opt/data/employees_raw.csv")

print("Raw count:", df_raw.count())
df_raw.show(5, truncate=False)

from pyspark.sql.functions import col

# Remove records with nulls in critical fields or are not null in postgresql sink table
df_valid = df_raw.filter(
    col("employee_id").isNotNull() &
    col("email").isNotNull() &
    col("hire_date").isNotNull()
)

#remove primary key deduplication
df_dedup = df_valid.dropDuplicates(["employee_id"])

from pyspark.sql.functions import lower

# Normalize email to lowercase
df_email_norm = df_dedup.withColumn(
    "email",
    lower(col("email"))
)

email_regex = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"

# Validate email format
df_email_valid = df_email_norm.filter(
    col("email").rlike(email_regex)
)

# Remove duplicate emails
df_email_unique = df_email_valid.dropDuplicates(["email"])


from pyspark.sql.functions import regexp_replace

# Clean salary field by removing $ and , characters
df_salary_clean = df_email_valid.withColumn(
    "salary_clean",
    regexp_replace(col("salary"), "[$,]", "")
)

# Cast salary to double 
df_salary_cast = df_salary_clean.withColumn(
    "salary",
    col("salary_clean").cast("double")
).drop("salary_clean")

# df_salary_cast = df_salary_cast.filter(col("salary").isNotNull())


from pyspark.sql.functions import to_date

# Convert hire_date and birth_date to date type
df_dates = df_salary_cast \
    .withColumn("hire_date", to_date(col("hire_date"))) \
    .withColumn("birth_date", to_date(col("birth_date")))


from pyspark.sql.functions import current_date

# Remove records with hire_date in the future
df_dates_valid = df_dates.filter(
    col("hire_date") <= current_date()
)


from pyspark.sql.functions import initcap

# Standardize name capitalization
df_names = df_dates_valid \
    .withColumn("first_name", initcap(col("first_name"))) \
    .withColumn("last_name", initcap(col("last_name")))

# Standardize department and status capitalization
df_norm = df_names \
    .withColumn("department", initcap(lower(col("department")))) \
    .withColumn("status", initcap(lower(col("status"))))

from pyspark.sql.functions import concat_ws

# Dataset enrichment
# Create full_name column
df_fullname = df_norm.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("last_name"))
)

from pyspark.sql.functions import split

# Extract email domain
df_email_domain = df_fullname.withColumn(
    "email_domain",
    split(col("email"), "@").getItem(1)
)

from pyspark.sql.functions import floor, months_between, current_date

# Calculate age and tenure
df_age = df_email_domain.withColumn(
    "age",
    floor(months_between(current_date(), col("birth_date")) / 12)
)

# Calculate tenure in years with one decimal place
df_tenure = df_age.withColumn(
    "tenure_years",
    floor(months_between(current_date(), col("hire_date")) / 12 * 10) / 10
)

from pyspark.sql.functions import when

# Create salary_band based on salary ranges
df_salary_band = df_tenure.withColumn(
    "salary_band",
    when(col("salary") < 50000, "Junior")
    .when((col("salary") >= 50000) & (col("salary") <= 80000), "Mid")
    .otherwise("Senior")
)


df_final = df_salary_band.select(
    "employee_id",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "email_domain",
    "hire_date",
    "job_title",
    "department",
    "salary",
    "salary_band",
    "manager_id",
    "address",
    "city",
    "state",
    "zip_code",
    "birth_date",
    "age",
    "tenure_years",
    "status"
)

from pyspark.sql.functions import col

df_typed = df_final \
    .withColumn("employee_id", col("employee_id").cast("int")) \
    .withColumn("manager_id", col("manager_id").cast("int")) \
    .withColumn("age", col("age").cast("int")) \
    .withColumn("salary", col("salary").cast("double")) \
    .withColumn("tenure_years", col("tenure_years").cast("double"))

df_typed = df_typed.filter(col("employee_id").isNotNull())


jdbc_url = "jdbc:postgresql://postgres:5432/employees_db"

db_properties = {
    "user": "employee_user",
    "password": "employee_pass",
    "driver": "org.postgresql.Driver"
}

mode = "append"

print("Final row count:", df_typed.count())
print("Writing data to PostgreSQL...")

# Write cleaned data to PostgreSQL
df_typed.write \
    .jdbc(
        url=jdbc_url,
        table="employees_clean",
        mode="append",
        properties=db_properties
    )


