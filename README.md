# Real-Time Spark Streaming Application



## Technical Details

### Spark: 
•	spark-2.4.7-bin-hadoop2.7
•	Spark run on local[*]

#### Spark-Submit Command: 
spark-submit2 --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --conf "spark.driver.extraClassPath=C:\Users\Admin\.m2\repository\org\apache\spark\spark-sql-kafka-0-10_2.11\2.4.7\spark-sql-kafka-0-10_2.11-2.4.7.jar" --conf "spark.driver.extraClassPath=C:\Users\Admin\.m2\repository\org\apache\kafka\kafka-clients\2.2.1\kafka-clients-2.2.1.jar" --conf "spark.executor.extraClassPath=C:\Users\Admin\.m2\repository\org\apache\kafka\kafka-clients\2.2.1\kafka-clients-2.2.1.jar" --conf "spark.executor.extraClassPath=C:\Users\Admin\.m2\repository\org\apache\spark\spark-sql-kafka-0-10_2.11\2.4.7\spark-sql-kafka-0-10_2.11-2.4.7.jar,C:\Users\Admin\.m2\repository\org\apache\kafka\kafka-clients\2.2.1\kafka-clients-2.2.1.jar" --jars C:\Users\Admin\.m2\repository\org\apache\spark\spark-streaming-kafka_2.11\1.6.3\spark-streaming-kafka_2.11-1.6.3.jar --class com.enbd.assesment.loanacc.core.LoanAccApp2  D:\Amruth\Learning\PIP\ENBD3\enbd-assesment-loanacc\target\enbd-assesment-loanacc-1.0.0-jar-with-dependencies.jar

### Kafka	
•	kafka_2.11-2.2.1
•	Topics: 
  loan
  account




# Testing

## Batch 1: 

### Input Messages

    Loan: 
        {   "LoanId": 5,   "AccountId": 5,   "Amount": 50000.00 }
        {   "LoanId": 6,   "AccountId": 6,   "Amount": 60000.00 }
        {   "LoanId": 7,   "AccountId": 7,   "Amount": 70000.00 }
        {   "LoanId": 8,   "AccountId": 8,   "Amount": 80000.00 }

    Account: 
        {"AccountId": 5,"AccountType": 1}
        {"AccountId": 6,"AccountType": 1}
        {"AccountId": 7,"AccountType": 2}
        {"AccountId": 8,"AccountType": 2}

### Output: 
   Check the Output1.png file for screen shot of the testing result attached in the repository




## Batch 2: 

### Input Messages

    Loan: 
        {   "LoanId": 6,   "AccountId": 6,   "Amount": 60000.00 }
        {   "LoanId": 7,   "AccountId": 7,   "Amount": 70000.00 }

    Account: 
        {"AccountId": 6,"AccountType": 1}
        {"AccountId": 7,"AccountType": 2}

### Output: 
Check the Output2.png file for screen shot of the testing result attached in the repository   
   
   
 # App Details : 
 
 1. Class: com.enbd.assesment.loanacc.core.LoanAccApp
  This is the working application which is used to call in the spark-submit, covers most of the requiremnts.
  
 2. Class: com.enbd.assesment.loanacc.core.LoanAccApp2
  - This application is an attempt to achieve aggregation of streams with differnt groupBy (One - AccountType unbounded output and another - window)
  - This was not completely successful as i started getting the error -------->  "Multiple streaming aggregations are not supported with streaming DataFrames/Datasets"
  - However, this app covers Watermarking, Handling Late data and maintain running total which is also desired.
  

