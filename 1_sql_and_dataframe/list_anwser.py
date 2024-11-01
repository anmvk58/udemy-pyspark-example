from pyspark.sql.functions import *

def question_1(spark, df):
    # Q1. How many distinct types of calls were made to the Fire Department?
    # method 1 using spark sql:
    q1_sql_df = spark.sql("""
            select 
                count(distinct CallTypeGroup) as distinct_call_type_count 
            from
                fire_service_calls_view
            where 
                CallTypeGroup is not null
        """)
    q1_sql_df.show()

    # method 2 using dataframe
    q1_df = df.where(df.CallTypeGroup.isNotNull()) \
        .select("CallTypeGroup") \
        .distinct()

    print('number of distinct CallTypeGroup = %d' % q1_df.count())


def question_2(spark, df):
    # Q2. What were distinct types of calls made to the Fire Department?
    # method 1 using spark sql:
    q2_sql_df = spark.sql("""
                select 
                    distinct CallTypeGroup 
                from
                    fire_service_calls_view
                where 
                    CallTypeGroup is not null
            """)
    q2_sql_df.show()

    # method 2 using dataframe
    q2_df = df.where(df.CallTypeGroup.isNotNull()) \
        .select("CallTypeGroup") \
        .distinct()

    q2_df.show()


def question_3(spark, df):
    # Q3. Find out all response for NumberOfAlarms greater than 3 times ?
    # method 1: using spark sql
    q3_sql_df = spark.sql("""
                select 
                    CallNumber, RowID, NumberOfAlarms
                from 
                    fire_service_calls_view
                where 
                    NumberOfAlarms > 3
                order by 
                    NumberOfAlarms desc
        """)
    q3_sql_df.show()

    # method 2: using dataframe
    q3_df = df.where(df.NumberOfAlarms > 3) \
        .select("CallNumber", "RowID", "NumberOfAlarms") \
        .orderBy("NumberOfAlarms", ascending=False)

    q3_df.show()


def question_4(spark, df):
    # Q4. What were the most common call types?
    # method 1: using spark sql
    q4_sql_df = spark.sql("""
        select 
            CallTypeGroup, count(1) as count
        from 
            fire_service_calls_view
        where 
            CallTypeGroup is not null 
        group by 
            CallTypeGroup
        order by 
            count desc
    """)
    q4_sql_df.show()

    # method 2: using dataframe
    q4_df = df.where(df.CallTypeGroup.isNotNull()) \
        .select("CallTypeGroup") \
        .groupBy("CallTypeGroup") \
        .count() \
        .orderBy("count", ascending=False)

    q4_df.show()


def question_5(spark, df):
    # Q5. What zip codes accounted for most common calls?
    # method 1: using spark sql
    q5_sql_df = spark.sql("""
        select 
            CallTypeGroup, ZipCode, count(*) as count
        from 
            fire_service_calls_view
        where 
            CallTypeGroup is not null
        group by 
            CallTypeGroup, ZipCode
        order by 
            count desc
    """)
    q5_sql_df.show()

    q5_df = df.where(df.CallTypeGroup.isNotNull()) \
        .select("CallTypeGroup", "ZipCode") \
        .groupBy("CallTypeGroup", "ZipCode") \
        .count() \
        .orderBy("count", ascending=False)

    q5_df.show()


def question_6(spark, df):
    # Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103
    # method 1: using spark SQL
    q6_sql_df = spark.sql("""
        select 
            distinct Neighborhooods, Zipcode
        from 
            c
        where 
            ZipCode == 94102 or ZipCode == 94103
    """)
    q6_sql_df.show()

    # method 2: using dataframe
    q6_df = (df.select("Neighborhooods", "Zipcode")
             .where((col("ZipCode")==94102) | (col("ZipCode")==94103))
             # .where((df.ZipCode==94102) | (df.ZipCode==94103))
             .distinct())
    q6_df.show()


def question_7(spark, df):
    # Q7. What was the sum of all calls, average, min and max of the response times for calls?
    # method 1: using spark SQl
    q7_sql_df = spark.sql("""
        select 
            sum(NumberOfAlarms), avg(FinalPriority), min(FinalPriority), max(FinalPriority)
        from 
            fire_service_calls_view
    """)
    q7_sql_df.show()

    # method 2: using dataframe
    q7_df = df.select(
        sum("NumberOfAlarms"),
        avg("FinalPriority"),
        min("FinalPriority"),
        max("FinalPriority")
    )
    q7_df.show()


def question_8(spark, df):
    # Q8. How many distinct years of data is in the CSV file?
    # method 1: using spark SQL
    q8_sql_df = spark.sql("""
        select
            distinct year(CallDate) as year_num
        from
            fire_service_calls_view
        order by 
            year_num desc
    """)
    q8_sql_df.show()

    # method 2
    q8_df = df.select(year("CallDate").alias("year_num")).distinct().orderBy("year_num", ascending=False)
    q8_df.show()


def question_9(spark, df):
    # Q9. What week of the year in 2018 had the most fire calls?
    # method 1: using spark SQL
    q9_sql_df = spark.sql("""
        select 
            weekofyear(CallDate) as week_year,
            count(1) as count
        from 
            fire_service_calls_view
        where 
            year(CallDate) == 2018
        group by 
            week_year
        order by 
            count desc
    """)
    q9_sql_df.show()

    # method 2 : using spark dataframe
    q9_df = (df.select(weekofyear("CallDate").alias("week_year"))
             .where(year("CallDate")==2018)
             .groupBy("week_year")
             .count()
             .orderBy("count", ascending=False)
             )
    q9_df.show()


def question_10(spark, df):
    # Q10. What neighborhoods in San Francisco had the worst response time in 2018?
    # Method 1: spark SQL
    q10_sql_df = spark.sql("""
        select 
            Neighborhooods, FinalPriority
        from 
            fire_service_calls_view 
        where 
            year(CallDate) == 2018
    """)

    q10_sql_df.show()

    # Method 2: spark dataframe
    q10_df = df.where(year(df.CallDate)==2018).select("Neighborhooods", "FinalPriority")

    q10_df.show()