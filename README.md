Spark Server
===========================================================================

Spark Server is a worksharing framework running on top of Apache Spark engine, allowing pipelining jobs to reduce the running time in medium to large cluster computing system for Big Data. Some optimizations behind Spark Server are: sharing scan, sharing computation, cache detection, ... Additionally, Spark Server also balances the trade-off between sharing computation (system throughput) and  meeting soft deadlines (job latency) given by the user.

##Online documentation
You can find the latest documentation, guide on [Project website](http://wwww.ProjectWebsite), [Project internals](project-internals/PROJECT-INTERNALS.md) and [API docs](https://cdn.rawgit.com/ngtrkhoa/spark-server/master/api-docs/index.html)

This README file only contains basic setup instructions.

##Project structure
Project is organized as follows:
- sparksql-server: worksharing framework
- modified-spark: 
    + a modified build of apache spark 1.3.1
    + a dependency of sparksql-server and spark-client

##Building project
- Clients' applications and sparksql-server will have the modified spark build (in spark-assembly folder) as their dependency
- Use Intellij to import and build project
- Client applications are built as normal way with using our modified-spark
- sparksql-server is built and run normally

##How to use SparkSQL Server
To bring SparkSQL in action, we need to know how to submit a Spark application to SparkSQL Server and how to deploy the server on the cluster:
At client side: Users write their Spark application normally as they often do. In stead of using the default Spark Library of Apache, users need to use our Spark Library from the Github repository. We need to modify the spark-submit command to provide information about SparkSQL Server. The new submit command that users need to use is:\\
./bin/spark-submit \
  --class <main-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  --sparksql-server <ip,sparksql-server-port,jar-server-port>
  --metadata <key1:value1 key2:value2 ...>
  ... # other options
  <application-jar> \
  [application-arguments]
The value for sparksql-server argument is splitted by comma, with the order of sparksql-serve IP, sparksql-server Port, jar-server Port.
SparkSQL Server is also a Spark Application, we just submit it as usual. The difference of SparkSQL Server to a normal Spark Application is it does not submit the applications immediately to the cluster but it waits until getting enough applications from users to do the optimizations.

##Running examples
[On progress]
##Running tests
[On progress]
