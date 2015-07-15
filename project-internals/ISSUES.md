Spark Server - Issues
===========================================================================

- lack of Unit test
- customized Spark-submit command
- trackable changes in Spark build
- **Scan sharing without in-memory caching**
    + Why we should do
        * Community need:
            - Multi-insert in Hive - https://issues.apache.org/jira/browse/HIVE-7503
            - JIRA pull request - https://issues.apache.org/jira/browse/SPARK-2688
        * The success of project
        * In comparison with RDD automatically caching approach: better in term of both cost and efficiency
            - I'm not doing iterative process
            - I just want to split my data
            - Caching still requires iterating whole in-memmory data multiple times
        * The trend of new API in Spark: asynchronous job submision
    + Challenge:
        * Pulling model: RDD compute function iterates whole partition at once. API do not support computing in single record
    + Solution:
        * Deffered execution of actions in RDD
        * Similar to MQO in Pig: With Split & Demultiplex operation
        * Introduce new type of RDD - a container of RDDs
        * Combine the execution of child RDDs sharing the same parent if possible
- Merging multiple jobs is not always beneficial. We're sacrifying the degree of parallelism. Automatically adjust the number of reducers can help reducing this kind of impact
- Many open questions on merging multiple jobs
- Scheduler's role