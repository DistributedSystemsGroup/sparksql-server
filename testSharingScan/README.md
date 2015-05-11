WORDCOUNT - TEST SHARING SCAN

Simple WordCount application on Spark to test sharing scan using caching.

Arguments:
- Number of WordCount Jobs
- Running Mode: SEQ for sequential, CON for concurrent
- Caching: 1 for caching, 0 for no Caching
- Input path
- Output path

Result:
![Image of result]
(https://lh3.googleusercontent.com/Ncn4fr8wpDSqbAxuMVaCEli2pvGLZXtdPNjPrvm7UkTAYIlUqvZzhbBv4VkvpCPiEI_97ykjY4AedAw=w1656-h799-rw)
With number of jobs are: 2, 5, 10
- caching-jobs total runtime > non-caching-jobs total runtime
- sequential-jobs total runtime (using FIFO) > concurrent-jobs total runtime (using FAIR + multithreading)
