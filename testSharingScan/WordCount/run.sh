hadoop fs -rm -r wc_spark_*

#echo "2 SEQ 0"
#YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 2 SEQ 0 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-2-seq-0
#hadoop fs -rm -r wc_spark_*

echo "2 CON 0"
YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 2 CON 0 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-2-con-0
hadoop fs -rm -r wc_spark_*

#echo "2 SEQ 1"
#YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 2 SEQ 1 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-2-seq-1
#hadoop fs -rm -r wc_spark_*

echo "2 CON 1"
YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 2 CON 1 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-2-con-1
hadoop fs -rm -r wc_spark_*


#echo "5 SEQ 0"
#YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 5 SEQ 0 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-5-seq-0
#hadoop fs -rm -r wc_spark_*

echo "5 CON 0"
YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 5 CON 0 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-5-con-0
hadoop fs -rm -r wc_spark_*

#echo "5 SEQ 1"
#YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 5 SEQ 1 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-5-seq-1
#hadoop fs -rm -r wc_spark_*

echo "5 CON 1"
YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 5 CON 1 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-5-con-1
hadoop fs -rm -r wc_spark_*


#echo "10 SEQ 0"
#YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 10 SEQ 0 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-10-seq-0
#hadoop fs -rm -r wc_spark_*

echo "10 CON 0"
YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 10 CON 0 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-10-con-0
hadoop fs -rm -r wc_spark_*

#echo "10 SEQ 1"
#YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 10 SEQ 1 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-10-seq-1
#hadoop fs -rm -r wc_spark_*

echo "10 CON 1"
YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --num-executors 34 --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 10 CON 1 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-10-con-1
hadoop fs -rm -r wc_spark_*

