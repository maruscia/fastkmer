# Kmer Counting with Spark


## Dependencies:
- scala-sdk-2.10.6
- spark-assembly-2.0.0-hadoop2.7.0-SNAPSHOT.jar (see next section)
- FASTdoop-1.0.jar



### IntelliJ Azure plugin ###

Instructions on how to configure the IntelliJ Azure plugin to interoperate with HDinsight:
https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apache-spark-intellij-tool-plugin

Make sure to use the HDInsight spark-assembly-2.0.0-hadoop2.7.0-SNAPSHOT.jar which is *not*
available on official Maven/3rd party repositories.
It can also be downloaded directly from an HDInsight node.