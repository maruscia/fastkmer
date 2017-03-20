import Main.Configuration
import fastdoop.{FASTAlongInputFileFormat, PartialSequence}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import common.util._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object SparkKmerCounter {
  //val logger = Logger(LoggerFactory.getLogger("InputOutputFormatDriver"))


  def getSuperKmers(k: Int, m: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable,PartialSequence)]) : Iterator[(String,String)] = {

    var out = ListBuffer.empty[(String,String)]
    while (reads.hasNext)
    {
      val cur:String = reads.next._2.getValue.replaceAll("\n","")

      //initialize vars
      var min_s: Signature = minimumSignature(cur.substring(0,k),m,0,bothStrands)
      var super_kmer_start = 0

      cur.sliding(k, 1).zipWithIndex.foreach {

        case (s, i) =>

          if (i > min_s.pos) {

            //add superkmer
            out += ((min_s.value, cur.substring(super_kmer_start, i - 1 + k)))
            min_s = minimumSignature(s, m, i,bothStrands)
            super_kmer_start = i
          }
          else {
            val last = repr(s.takeRight(m),bothStrands)

            if (last < min_s.value) {

              //add superkmer
              out += ((min_s.value, cur.substring(super_kmer_start, i - 1 + k)))
              min_s = Signature(last, i + k - m)
              super_kmer_start = i

            }
          }

      }

      out += ((min_s.value, cur.substring(super_kmer_start, cur.length)))


    }
    // return Iterator[U]
    out.iterator
  }


  /*

  TODO: next
  def extractKmers(k: Int)(bin: Iterator[(String,ArrayBuffer[String])]) : Iterator[(String,Int)]

  */

  def minimumSignature(s: String, m: Int, s_starting_pos:Int, canonical: Boolean): Signature = {
    val tuple = s.sliding(m,1).zipWithIndex.map{ case (str,i) => (repr(str,canonical),i)}.min
    Signature(tuple._1,tuple._2 + s_starting_pos)
  }


  def executeJob(spark: SparkSession, input: String, output: String): Unit = {

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", Configuration.K.toString)

    //I guess we should be able to set mapred.max(min).split.size to a desired value
    // then the split size is calculated with this formula: max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    // for what concerns partitions, spark creates a single partition for a single input split, so this is safe

    val FASTfile = input
    println(FASTfile)
    println(Configuration.K)
    println(Configuration.N)
    val broadcastK = sc.broadcast(Configuration.K)
    val broadcastN = sc.broadcast(Configuration.N)
    val broadcastM = sc.broadcast(Configuration.M)
    val broadcastB = sc.broadcast(Configuration.BOTHSTRANDS)

    //FASTQ: sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord]), conf)

    val sequencesRDD =
      sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

    val readPartitions = sequencesRDD.mapPartitions(getSuperKmers(broadcastK.value,broadcastM.value,broadcastB.value)).aggregateByKey(new ArrayBuffer[String]())((buff:ArrayBuffer[String],s) => buff += s,(buf1,buf2) => buf1 ++= buf2)


    //readPartitions.foreachPartition(_._2.sliding(broadcastK.value, 1).toIterator)
    val allTopN = readPartitions.take(broadcastN.value)
    //sc.parallelize(allTopN, 1).saveAsTextFile(output)
    // print out top-N kmers
    allTopN.foreach(println)

  }
}
