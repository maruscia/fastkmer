import Main.Configuration
import common.sorting.RadixLSDSort
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
            val last = mMerRepr(s.takeRight(m),bothStrands)

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
    out.iterator
  }




  def extractKXmers(k: Int, x: Int)(bin: Iterator[(String,ArrayBuffer[String])]) : Iterator[(String,Int)] = {

    var superkmers = bin.flatMap(_._2)
    // Array that will contain all (k,x)-mers (R)
    var unsortedR = Array.fill[ArrayBuffer[String]]( x + 1)(new ArrayBuffer[String])

    //output buffer
    var out = ListBuffer.empty[(String,Int)]

    var lastOrientation = -1
    var orientation = -1
    var runLength = 0
    var runStart = 0
    var sk:String = null

    while(superkmers.hasNext){
      sk = superkmers.next()

      lastOrientation = -1
      orientation = -1
      runLength = 0

      // the length of a run is 1 if i have a k-mer
      // 2 if i have a k+1 mer
      // 3 if i have a k+2 mer
      // ...

      for ((s, i) <- sk.sliding(k,1).zipWithIndex) {
        orientation = getOrientation(s)

        //check if we need to output
        if (orientation == lastOrientation) {

          runLength += 1

          //if we have reached the biggest subsequence possible, must output it
          if (runLength == x + 1) {
            unsortedR(runLength -1).append(getCanonicalSubstring(sk,runStart, runStart  + k + runLength - 1,orientation))
            runLength = 0
            runStart = i
            lastOrientation = -1
          }
        }

        else{
          //last orientation was different, must output previous sequence
          if(lastOrientation != -1) {
            unsortedR(runLength-1).append(getCanonicalSubstring(sk,runStart, runStart  + k + runLength - 1,lastOrientation))
          }

          //increase run length
          runLength = 1
          runStart = i
          lastOrientation = orientation
        }
      }
      if(runLength >0) {
        unsortedR(runLength - 1).append(getCanonicalSubstring(sk,runStart, runStart  + k + runLength - 1,lastOrientation))
      }
    }

    superkmers = null //hope GC will take care of this fixme: check why I can't call the GC explicitly (maybe configuring an aggressivity timeout)

    //////// TODO sorting must be in place /////////
    var sortedR = ArrayBuffer[Array[String]]()
    unsortedR.foreach(println)
    for (i <- unsortedR.indices)
      sortedR += RadixLSDSort(unsortedR(i), k + i)
    ////////////////////////////////////////////////

    val heap = priorityQueueWithIndexes(sortedR.toArray,k)

    require(heap.nonEmpty)
    var index: RIndex = null
    var last_kmer: String = null
    var last_kmer_cnt = 0

    var done = false
    while (heap.nonEmpty){
      index = heap.dequeue()
      if (index.pointedKmer == last_kmer)
        last_kmer_cnt += 1
      else {

        if(last_kmer != null) { //kmer has changed, send out previous one with completed count
          out.append((last_kmer, last_kmer_cnt))

        }
        last_kmer = index.pointedKmer
        last_kmer_cnt = 1
      }
      index.advance()

      if(!index.exhausted) //if it still has kmers to read
        heap.enqueue(index) //put it back in the heap
    }
    out.append((last_kmer, last_kmer_cnt))
    out.toIterator


  }




  def executeJob(spark: SparkSession, input: String, output: String): Unit = {

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", Configuration.K.toString)

    //I guess we should be able to set mapred.max(min).split.size to a desired value
    // then the split size is calculated with this formula: max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    // for what concerns partitions, spark creates a single partition for a single input split, so this is safe

    val FASTfile = input
    println(this.getClass.getSimpleName)
    println(FASTfile)
    println(Configuration.K)
    println(Configuration.N)
    val broadcastK = sc.broadcast(Configuration.K)
    val broadcastN = sc.broadcast(Configuration.N)
    val broadcastX = sc.broadcast(Configuration.X)
    val broadcastM = sc.broadcast(Configuration.M)
    val broadcastB = sc.broadcast(Configuration.BOTHSTRANDS)

    //FASTQ: sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord]), conf)

    val sequencesRDD =
      sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

    val readPartitions = sequencesRDD.mapPartitions(getSuperKmers(broadcastK.value,broadcastM.value,broadcastB.value)).aggregateByKey(new ArrayBuffer[String]())((buff:ArrayBuffer[String],s) => buff += s,(buf1,buf2) => buf1 ++= buf2)

    val sortedKmerCounts = readPartitions.mapPartitions(extractKXmers(broadcastK.value,broadcastX.value))//.mapPartitions(_.toList.sortBy(r => (r._2,r._1)).takeRight(broadcastN.value).toIterator)

    val all = sortedKmerCounts.sortBy(_._1)//(r => (r._2,r._1),false).take(broadcastN.value)

    all.saveAsTextFile(output)

    //all.foreach(println)

  }
}
