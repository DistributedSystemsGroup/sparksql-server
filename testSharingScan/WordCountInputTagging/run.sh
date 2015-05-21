export SPARK_EXECUTOR_INSTANCES=15
export SPARK_EXECUTOR_CORES=4

hadoop fs -rm -r wc_spark_*

YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --driver-memory 4g --executor-memory 4g --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 2 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-2
hadoop fs -rm -r wc_spark_*

YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --driver-memory 4g --executor-memory 4g --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 5 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-5
hadoop fs -rm -r wc_spark_*

YARN_CONF_DIR=/etc/hadoop/conf/ spark-submit --driver-memory 4g --executor-memory 4g --class fr.eurecom.dsg.WordCount --deploy-mode client --master yarn ./target/wordcount-0.0.1-SNAPSHOT.jar 10 hdfs://bigdoop-1:/laboratory/gutenberg_big.txt hdfs://bigdoop-1:/user/worksharing/wc_spark_ > wc-10
hadoop fs -rm -r wc_spark_*
