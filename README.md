Spark Server
===========================================================================

Spark Server is a worksharing framework running on top of Apache Spark engine, allowing pipelining jobs to reduce the running time in medium to large cluster computing system for Big Data. Some optimizations behind Spark Server are: sharing scan, sharing computation, cache detection, ... Additionally, Spark Server also balances the trade-off between sharing computation (system throughput) and  meeting soft deadlines (job latency) given by the user.

##Online documentation
You can find the latest documentation, guide on [Project website](http://wwww.ProjectWebsite), [Project internals](project-internals/PROJECT-INTERNALS.md) and [API docs](https://cdn.rawgit.com/ngtrkhoa/spark-server/master/api-docs/index.html)

This README file only contains basic setup instructions.

##Project structure
Project is organized as follows:
- sparksql-server: worksharing framework
- spark-client (SimpleAppClient): A simple client that submits jobs to sparksql-server
- spark-1.3.1: 
    + a modified build of apache spark 1.3.1
    + a dependency of sparksql-server and spark-client
    + jar file: https://drive.google.com/file/d/0B9PhrkrQAJcuWGh2cVlOaElLMWs/view?usp=sharing

##Building project
- Clients' applications and sparksql-server will have the modified spark build (in spark-assembly folder) as their dependency
- Use Intellij to import and build project
- Command to build: [On progress]

##Running examples
[On progress]
##Running tests
[On progress]