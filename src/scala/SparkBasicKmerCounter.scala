import Main.Configuration
import fastdoop._
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import common.util._
import org.slf4j.LoggerFactory



object SparkBasicKmerCounter {
  //val logger = Logger(LoggerFactory.getLogger("InputOutputFormatDriver"))

  def executeJob(spark: SparkSession, input: String, output: String): Unit = {

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", Configuration.K.toString)

    //I guess we should be able to set mapred.max(min).split.size to desired values
    // then the split size is calculated with this formula: max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    // then we come to the partitions: spark creates a single partition for a single input split, so this is safe


    val FASTfile = input
    println(this.getClass.getSimpleName)
    println(FASTfile)
    println(Configuration.K)
    println(Configuration.N)
    val broadcastK = sc.broadcast(Configuration.K)//

    val broadcastM = sc.broadcast(Configuration.M)
    val broadcastN = sc.broadcast(Configuration.N)
    val broadcastCanonical = sc.broadcast(Configuration.BOTHSTRANDS)



    val sequencesRDD = //FASTQ: sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord])//, conf)//
      sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

    val kmers = sequencesRDD.flatMap(_._2.getValue.replaceAll("\n","").sliding(broadcastK.value, 1).map(c => (repr(c,broadcastCanonical.value), 1)))

    // find frequencies of kmers
    val kmersGrouped = kmers.reduceByKey(_ + _) //reduceByKey is more efficient because it reduces on each node before shuffling

    val partitions = kmersGrouped.mapPartitions(_.toList.sortBy(_._1).toIterator)//(r => (r._2,r._1)).takeRight(broadcastN.value).toIterator)

    val all = partitions.sortBy(_._1)//(r => (r._2,r._1),false).take(broadcastN.value)

    all.saveAsTextFile(output)
    //all.foreach(println)

  }
}