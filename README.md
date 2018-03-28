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
FASTdoop is not yet available on maven repositories. To import the relevant jar into your local maven repository, run the following command:
```
mvn install:install-file -Dfile=/path/to/FASTdoop-1.0.jar -DgroupId=org.fastdoop -DartifactId=fastdoop -Dversion=1.0 -Dpackaging=jar
```


### IntelliJ Azure plugin ###
This project is ready to be run on top of a Microsoft Azure HDInsight cluster.
Optionally, the cluster job can be submitted, run, and debugged directly from your machine using IntelliJ IDEA or Eclipse.

Instructions on how to configure the IntelliJ Azure plugin to interoperate with HDinsight can be found at:
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
|_B_| number of bins|
|_useHT_ | 1 for HT based implementation, or 0|
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
spark-submit --master yarn --deploy-mode cluster  --num-executors <executors> --executor-cores <cores> --driver-memory 1g --executor-memory <Xg> --jars /path/to/fastutil-7.2.1.jar,/path/to/FASTdoop-1.0.jar --class skc.test.TestKmerCounter SKC-1.0-SNAPSHOT.jar  28 10 3 2048 0 0 hdfs://mycluster/tests/input/ggallus.fasta /mycluster/tests/output/ gallus 1 0 0 0

```

remember to put all the external jars on the node where spark-submit is invoked (`/path/to/<jar>.jar`), so that they can be deployed on all worker nodes by Spark.


### Handling multiple sequences (experimental, under development) ###

The package multisequence contains a prototypical implementation of multiple sequence distance computation based on exact k-mers.
The main runnable class is `TestMultisequenceKmerCounter`, and assumes as _inputPath_ a file containing reads from a set of sequences to be compared.
Each sequence is assumed to be pre-tagged by a read descriptor, as highlighted in the following example:

**sequences.fasta**

<pre>
<span style="color:blue;">SRR197985</span>.1 HWUSI-EAS687_61DAJ:3:1:1046:16470 length=200
AAGCAGGGGGTTGGTGCTGGCANGCAGTCTCAGGGCGTTTNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
NNNNNNNNNNNNAAACATTAATGTTAAAATGACACAAATCCTTACCCGGTTNGCTATCTTTTCCNNNNNNNNANNNNTGNNGNTNGCNNNAATGCCNTTG
<span style="color:green;">SRR956987</span>.1 HWI-ST571:185:D111MACXX:2:1101:1213:2216 length=101
TGGACTCTTCTGCTTGGCACGGGACTGTTGATTCAATCGGGTCTTCGCTCCTTTTTTAGTCCACCTTTTC
GGCTTATGCCTATGNNNTNGTTTCGAAAGCT
<span style="color:green;">SRR956987</span>.2 HWI-ST571:185:D111MACXX:2:1101:1213:2216 length=101
GTNNNNGACGAATGNNNCNNTCTTATGCNTNTTNGGANNNNCGACGGANTATTTNNNNCTTTTTTCTCNN
CGNCCGAAACCACTTTGACTGAGATTGNNNG
<span style="color:blue;">SRR197985</span>.2 HWUSI-EAS687_61DAJ:3:1:1046:1308 length=200
GGCACCATGGACTGCACGGACCCATGTGNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
NNNNNNNNNNNNGTGGGGTGTCCCACAAGGGTCCGTGCAGTCCAGGGTACCNCAGGTAGGGTCTCNNNNNNNTNNNCTGCCCCATCTGCNTCCCAGNACA
<span style="color:green;">SRR956987</span>.3 HWI-ST571:185:D111MACXX:2:1101:1019:2221 length=101
NGTGGTTCCGGAGAATCCAGCTACAGGAGAACCAGGAACGGAGAGCTCTCCCCCTTTTTCCGCCCGACTC
TTTGGTCTTAAGAATNCTGGTTTTAAGAACN
...
</pre>

Under `java/multiseq` package can be found many distance implementations. By default this prototype assumes Squared euclidean distance which can be computed autonomously and incrementally for each bin.


Currently, only partial distances are calculated inside bins but not yet aggregated and saved to disk.