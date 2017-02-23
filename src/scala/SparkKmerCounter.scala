import Main.Configuration
import fastdoop.{FASTAlongInputFileFormat, PartialSequence}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import common.util._

import scala.collection.mutable.ListBuffer


object SparkKmerCounter {
  //val logger = Logger(LoggerFactory.getLogger("InputOutputFormatDriver"))


  def getSuperKmers(k: Int, m: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable,PartialSequence)]) : Iterator[(String,String)] = {

    var out = ListBuffer.empty[(String,String)]
    while (reads.hasNext)
    {
      val cur:String = reads.next._2.getValue.replaceAll("\n","")

      //initialize vars
      var min_s: Signature = minimum_signature(cur.substring(0,k),m,0,bothStrands)
      var super_kmer_start = 0

      cur.sliding(k, 1).zipWithIndex.foreach {

        case (s, i) =>
          println(min_s)

          if (i > min_s.pos) {
            //add superkmer
            out += ((min_s.value, cur.substring(super_kmer_start, i - 1 + k)))
            min_s = minimum_signature(s, m, i,bothStrands)
            super_kmer_start = i
          }
          else {
            val last = s.takeRight(m)
            if (last < min_s.value) {

              min_s = Signature(last, i + k - m)

            }
          }

        }

      out += ((min_s.value, cur.substring(super_kmer_start, cur.length)))


    }
    // return Iterator[U]
    out.iterator
  }

  def minimum_signature(s: String, m: Int,s_starting_pos:Int,canonical: Boolean): Signature = {
    val tuple = s.sliding(m,1).zipWithIndex.foldLeft(("Z",-1))((x,y) => {
      val cy = (if(canonical) canonical_version(y._1) else y._1 , y._2)
      if ((if(canonical) canonical_version(x._1) else x._1) < cy._1) x else cy
    })
    Signature(tuple._1,tuple._2 + s_starting_pos)
  }

  def executeJob(spark: SparkSession, input: String, output: String): Unit = {

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", Configuration.K.toString)

    //I guess we should be able to set mapred.max(min).split.size to desired values
    // then the split size is calculated with this formula: max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    // then we come to the partitions: spark creates a single partition for a single input split, so this is safe

    val FASTfile = input
    println(FASTfile)
    println(Configuration.K)
    println(Configuration.N)
    val broadcastK = sc.broadcast(Configuration.K)//
    val broadcastN = sc.broadcast(Configuration.N)
    val broadcastM = sc.broadcast(Configuration.M)
    val broadcastB = sc.broadcast(Configuration.BOTHSTRANDS)





    val sequencesRDD = //sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord])//, conf)//
      sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

    val readPartitions = sequencesRDD.mapPartitions(getSuperKmers(broadcastK.value,broadcastM.value,broadcastB.value))
    // find frequencies of kmers
    val kmersGrouped = readPartitions.sortByKey() //reduceByKey is more efficient because it reduces on each node before shuffling

    val partitions = kmersGrouped.mapPartitions(_.toList.sortBy(_._2).takeRight(broadcastN.value).toIterator)

    val allTopN = partitions.sortBy(_._2, ascending=false, 1).take(broadcastN.value)
    //sc.parallelize(allTopN, 1).saveAsTextFile(output)
    // print out top-N kmers
    allTopN.foreach(println)

  }
}
