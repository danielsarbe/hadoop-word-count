# hadoop-word-count
A basic example of Hadoop Word Count implementation including the unit tests with MRUnit


1. Upload samples to hdfs
hadoop fs -fs hdfs://localhost:8020 -put src/main/resources/content/*.* content/

2. Compile and run
mvn clean hadoop:pack
hadoop jar target/hadoop-deploy/hadoop-word-count-hdeploy.jar org.hadoop.wordcount.RunWordCount