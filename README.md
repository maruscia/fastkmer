# Kmer Counting with Spark
This repository provides a distributed implementation of an exact Kmer Counter based on Spark

It is mainly coded in Scala, yet it entails and supports libraries written in java (see for example the distances under the java/multiseq package)

## Dependencies (pom.xml):
- scala-sdk-2.11.7
- spark-core_2.11
- spark-sql_2.11
- FASTdoop-1.0.jar (to be installed in local mvn repository, see next section)
- fastutil-7.2.1
- spark-assembly-2.0.0-hadoop2.7.0-SNAPSHOT.jar (optional, for use on a MS Azure cluster equipped with Azure HDInsight, see next section)


### Installing Fastdoop in local maven repository ###
Run the following command:
```
mvn install:install-file -Dfile=/path/to/FASTdoop-1.0.jar -DgroupId=org.fastdoop -DartifactId=fastdoop -Dversion=1.0 -Dpackaging=jar
```


### IntelliJ Azure plugin ###

Instructions on how to configure the IntelliJ Azure plugin to interoperate with HDinsight:
https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apache-spark-intellij-tool-plugin

Make sure to use the HDInsight spark-assembly-2.0.0-hadoop2.7.0-SNAPSHOT.jar which is *not*
available on official Maven/3rd party repositories.
It can also be downloaded directly from an HDInsight node.


## Project source overview ##

 
* [java](./src/java) | java libraries
    * [multiseq](./src/java/multiseq) multisequence distance function specifications (_experimental_)
* [scala](./src/scala) 
    * [skc](./src/scala/skc) | main project libraries
        * [multisequence](./src/scala/skc/multisequence) | multisequence support (_experimental_)
        * [test](./src/scala/skc/test) | runnable classes: `TestKmerCounter`, and `LocalTestKmerCounter`
        * [package.scala](./src/scala/skc/package.scala)
            * [util](./src/scala/skc/package.scala.util) | e.g., `Kmer`, `RIndex` classes and functions


## How to ##

**Compile**
```
mvn clean
mvn compile
```

**Package (only for cluster mode)**
```
mvn package
```

**Running locally using spark local mode**

`java -cp <CLASSPATH, including scala-library.jar> skc.test.LocalTestKmerCounter` _k m x useHT B sequenceType inputPath outputPath prefix write enableKryo useCustomPartitioner numPartitionTasks_

Parameter description (both for local and cluster mode):

| Name        | Meaning  |
| :-------------: |:-------------:|
|_k_ | kmers length|
|_m_ | signature length|
|_x_| _(k,x)_ mers compression factor|
|_useHT_ | 1 for HT based implementation, or 0|
|_B_| number of bins|
|_sequenceType_| 0 for short sequences, 1 for long|
|_inputPath_| dataset input path (HDFS or local)|
|_outputPath_| counts output path (HDFS or local)|
|_prefix_|custom output directory prefix|
|_write_| enable output|
|_enableKryo_| 1 to enable Kryo compression, or 0|
|_useCustomPartitioner_| 1 for partition balancing, or 0|
|_numPartitionTasks_| if partitioning, specifies number of partitions|


**Running in cluster mode (YARN) using spark-submit (example)**

Example for dataset ggallus.fasta located on HDFS at hdfs://mycluster/tests/input/ggallus.fasta

```
spark-submit --master yarn --deploy-mode cluster  --num-executors <executors> --executor-cores <cores> --driver-memory 1g --executor-memory <Xg> --jars /path/to/fastutil-7.2.1.jar /path/to/FASTdoop-1.0.jar --class skc.test.TestKmerCounter SKC-1.0-SNAPSHOT.jar  28 10 3 2048 0 0 hdfs://mycluster/tests/input/ggallus.fasta /mycluster/tests/output/ gallus 1 0 0 0

```

remember to put all the external jars on the node where spark-submit is invoked (`/path/to/<jar>.jar`), so that they can be deployed on all worker nodes by Spark.
