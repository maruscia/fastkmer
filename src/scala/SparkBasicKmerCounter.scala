import fastdoop._
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import common.util._
import org.slf4j.LoggerFactory



object SparkBasicKmerCounter {
  //val logger = Logger(LoggerFactory.getLogger("InputOutputFormatDriver"))

  def executeJob(spark: SparkSession, configuration: TestConfiguration): Unit = {

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", configuration.k.toString)

    //I guess we should be able to set mapred.max(min).split.size to a desired value
    // then the split size is calculated with this formula: max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    // for what concerns partitions, spark creates a single partition for a single input split, so this is safe

    val FASTfile = configuration.dataset
    val output = configuration.outputDir

    println(this.getClass.getSimpleName)
    println(configuration)

    val broadcastK = sc.broadcast(configuration.k)
    val broadcastX = sc.broadcast(configuration.x)
    val broadcastM = sc.broadcast(configuration.m)
    val broadcastC = sc.broadcast(configuration.canonical)
    val broadcastB = sc.broadcast(configuration.b)


    val sequencesRDD = //FASTQ: sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord])//, conf)//
      sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

    val kmers = sequencesRDD.flatMap(_._2.getValue.replaceAll("\n","").sliding(broadcastK.value, 1).map(c => (repr(c,broadcastC.value), 1)))

    // find frequencies of kmers
    val kmersGrouped = kmers.reduceByKey(_ + _) //reduceByKey is more efficient because it reduces on each node before shuffling

    val partitions = kmersGrouped.mapPartitions(_.toList.sortBy(_._1).toIterator)//(r => (r._2,r._1)).takeRight(broadcastN.value).toIterator)

    val all = partitions.sortBy(_._1)//(r => (r._2,r._1),false).take(broadcastN.value)

    all.saveAsTextFile(output)
    //all.foreach(println)

  }
}