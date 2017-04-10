

import common.sorting.RadixLSDSort
import fastdoop.{FASTAlongInputFileFormat, PartialSequence}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configured
import common.util._
import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import scala.util.Sorting.stableSort
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.RangePartitioner
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkKmerCounter {
    //val logger = Logger(LoggerFactory.getLogger("InputOutputFormatDriver"))


    def getSuperKmers(k: Int, m: Int, B: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, PartialSequence)]): Iterator[(Int, ArrayBuffer[String])] = {
      //debug: start datetime
      val start = new Date(System.currentTimeMillis())
      var t0 = System.nanoTime()/1000000

      def bin(s: String) = hash_to_bucket(s, B)
      var nreads = 0

      var out = Array.tabulate[(Int, ArrayBuffer[String])](B)(i => (i, new ArrayBuffer[String]))

      var t1 = t0
      var total:Long = 0

      while (reads.hasNext) {

        val cur: String = reads.next._2.getValue.replaceAll("\n", "")
        println("Processing read " + nreads + " - length: " + cur.length)
        if (cur.length >= k) {
          //initialize vars
          var min_s: Signature = minimumSignature(cur.substring(0, k), m, 0, bothStrands)
          var super_kmer_start = 0
          var s: String = null
          var N_pos = (-1, -1)
          var i = 0

          while (i < cur.length - k) {

            if(i % 250000 == 0) {
              t1 = System.nanoTime()/1000000
              println("[ " + "Elapsed: " + (t1-t0) +"ms ] i = " + i)
              t0 = t1
              total += t0
            }

            s = cur.substring(i, i + k)

            N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N', s) //DEBUG: not much influence

            if (N_pos._1 != -1) {
              //there's at least one 'N'

              if (super_kmer_start < i) {
                // must output a superkmer
                out(bin(min_s.value))._2 += cur.substring(super_kmer_start, i - 1 + k)

              }
              super_kmer_start = i + N_pos._2 + 1 //after last index of N
              i += N_pos._2 + 1
            }
            else {

              if (i > min_s.pos) {

                if (super_kmer_start < i) {
                  out(bin(min_s.value))._2 += cur.substring(super_kmer_start, i - 1 + k)

                  super_kmer_start = i
                }
                min_s = minimumSignature(s, m, i, bothStrands)

              }
              else {
                val last = mMerRepr(s.takeRight(m), bothStrands)

                if (last < min_s.value) {

                  //add superkmer
                  if (super_kmer_start < i) {
                    out(bin(min_s.value))._2 += cur.substring(super_kmer_start, i - 1 + k)

                    super_kmer_start = i
                  }
                  min_s = Signature(last, i + k - m)


                }
              }

              i += 1
            }
          }

          if (cur.length - super_kmer_start >= k)
            out(bin(min_s.value))._2 += cur.substring(super_kmer_start, cur.length)

        }
        nreads += 1
      }
      println("Finished getSuperKmers.")

      val end = new Date(System.currentTimeMillis())
      println("Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

      //filter empty bins, return iterator
      out.view.filter({case (_,arr) => arr.nonEmpty}).iterator
    }


    def evaluateBinBalance(path:String)(bins: Iterator[(Int, ArrayBuffer[String])]): Unit = {
      val arr = ArrayBuffer[(Int,Int)]()
      var outputFilename = "/balance_"

      var first = true

      while(bins.hasNext){

        val b:(Int,ArrayBuffer[String]) = bins.next

        if(first){
          outputFilename += b._1
          first = false
        }

        arr.append((b._1,b._2.size))
        println("Key: " + b._1 + " - size: " + b._2.size)
      }

      val outputStream = FileSystem.get(URI.create("hdfs://mycluster"), new Configuration()).create(new Path(path + outputFilename))
      val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
      var total = 0
      for(a <- arr.sortBy(_._2)){
        writer.write(a._1 + "," + a._2 + "\n")
        total += a._2
      }
      writer.write("Total: " +total + "\n")
      writer.close()
    }

    def extractKXmers(k: Int, x: Int, path: String)(bins: Iterator[(Int, ArrayBuffer[String])]): Unit = {
      //debug: start datetime

      val start = new Date(System.currentTimeMillis())
      println("["+ start + "] Started extractKXmers()")


      // Array that will contain all (k,x)-mers (R)
      var unsortedR: Array[ArrayBuffer[String]] = Array.fill[ArrayBuffer[String]](x + 1)(new ArrayBuffer[String])
      var sortedR: Array[Array[String]] = null


      var lastOrientation = -1
      var bin: Iterator[String] = null
      var orientation = -1
      var runLength = 0
      var runStart = 0
      var nElementsToSort = 0
      var kmer: String = null
      var sk: String = null

      val MB = 1024*1024
      var nBins = 0

      val r = Runtime.getRuntime

      while (bins.hasNext) {
        // for each bin


        val binStart = new Date(System.currentTimeMillis())

        val n = bins.next
        println("\n["+nBins + "] Examining bin: " + n._1 + " -  [Free memory: " + r.freeMemory()/MB + "MB]")
        bin = n._2.toIterator

        nElementsToSort = 0

        while (bin.hasNext) {



          //for each super-kmer
          sk = bin.next

          lastOrientation = -1
          orientation = -1
          runLength = 0


          // the length of a run is 1 if i have a k-mer
          // 2 if i have a k+1 mer
          // 3 if i have a k+2 mer
          // ...

          for (i <- 0 to sk.length - k) {

            kmer = sk.substring(i, i + k)

            orientation = getOrientation(kmer)

            //check if we need to output
            if (orientation == lastOrientation) {

              runLength += 1

              //if we have reached the biggest subsequence possible, must output it
              if (runLength == x + 1) {
                unsortedR(runLength - 1).append(getCanonicalSubstring(sk, runStart, runStart + k + runLength - 1, orientation))

                nElementsToSort += 1
                runLength = 0
                runStart = i
                lastOrientation = -1
              }
            }

            else {
              //last orientation was different, must output previous sequence
              if (lastOrientation != -1) {
                unsortedR(runLength - 1).append(getCanonicalSubstring(sk, runStart, runStart + k + runLength - 1, lastOrientation))

                nElementsToSort += 1
              }

              //increase run length
              runLength = 1
              runStart = i
              lastOrientation = orientation
            }
          }
          if (runLength > 0) {
            unsortedR(runLength - 1).append(getCanonicalSubstring(sk, runStart, runStart + k + runLength - 1, lastOrientation))

            nElementsToSort += 1
          }
        }

        //println("built unsorted array [Elapsed: " + (System.nanoTime()/1000000 - t0) + "ms]")

        //println("[Free memory: " + r.freeMemory()/MB+"mb]")

        println("> Final Size of toBeSortedR ArrayBuffer is :" +estimateSize(unsortedR))

        sortedR = unsortedR.view.map(_.toArray).force

        println(">> Size of sortedR Array is :" +estimateSize(sortedR))
        println(">> Sorting started (Array of " + nElementsToSort + " total elements.")

        for (i <- sortedR.indices){
          scala.util.Sorting.quickSort(sortedR(i)) // while we stick to Strings implementation, it doesn't make sense to use radix sort vs a comparison based method, for k > 10
          //println("called quicksort.")
        }

        println(">> Sorting finished - Free memory: " + r.freeMemory()/MB + "MB]")

        val heap = priorityQueueWithIndexes(sortedR, k)


        if (heap.nonEmpty) {

          val outputPath = path + "/bin" + n._1
          val outputStream = FileSystem.get(URI.create("hdfs://mycluster"), new Configuration()).create(new Path(outputPath))
          val writer = new BufferedWriter(new OutputStreamWriter(outputStream))

          var index: RIndex = null
          var last_kmer: String = null
          var last_kmer_cnt = 0

          println(">>> Started outputting k-mer counts")

          while (heap.nonEmpty) {
            index = heap.dequeue()
            if (index.pointedKmer == last_kmer)
              last_kmer_cnt += 1
            else {

              if (last_kmer != null) {

                //kmer has changed, send out previous one with completed count
                writer.write(last_kmer + "," + last_kmer_cnt + "\n")

              }
              last_kmer = index.pointedKmer
              last_kmer_cnt = 1
            }
            index.advance()

            if (!index.exhausted) //if it still has kmers to read
              heap.enqueue(index) //put it back in the heap
          }

          writer.write(last_kmer + "," + last_kmer_cnt + "\n")
          writer.close()


        }
        println(">>> Finished outputting - [Free memory: " + r.freeMemory()/MB + "MB]")
        for (el <- unsortedR)
          el.clear()

        println(">>> Called GC - [Free memory: " + r.freeMemory()/MB + "MB]")

        println("End processing bin. Elapsed: "+ getDateDiff(binStart,new Date(System.currentTimeMillis()),TimeUnit.SECONDS) +"s")
        nBins+=1
      }
      val end = new Date(System.currentTimeMillis())
      println("extractKXmers ended. Elapsed: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")
      }


    def executeJob(spark: SparkSession, configuration: TestConfiguration): Unit = {

      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      conf.set("k", configuration.k.toString)

      //I guess we should be able to set mapred.max(min).split.size to a desired value
      // then the split size is calculated with this formula: max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
      // for what concerns partitions, spark creates a single partition for a single input split, so this is safe

      val FASTfile = configuration.dataset


      println(this.getClass.getSimpleName)
      println(configuration)

      val broadcastK = sc.broadcast(configuration.k)
      val broadcastX = sc.broadcast(configuration.x)
      val broadcastM = sc.broadcast(configuration.m)
      val broadcastC = sc.broadcast(configuration.canonical)
      val broadcastB = sc.broadcast(configuration.b)
      val broadcastPath = sc.broadcast(configuration.outputDir)


      //FASTQ: sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord]), conf)

      val sequencesRDD =
        sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

      val readPartitions = sequencesRDD.mapPartitions(getSuperKmers(broadcastK.value, broadcastM.value, broadcastB.value, broadcastC.value)).reduceByKey(_ ++ _)//.aggregateByKey(new ArrayBuffer[String]())((buf1, buf2) => buf1 ++= buf2, (buf1, buf2) => buf1 ++= buf2)

      readPartitions.foreachPartition(extractKXmers(broadcastK.value, broadcastX.value,broadcastPath.value))//mapPartitions(extractKXmers(broadcastK.value, broadcastX.value)) //.mapPartitions(_.toList.sortBy(r => (r._2,r._1)).takeRight(broadcastN.value).toIterator)

      //readPartitions.foreachPartition(evaluateBinBalance(broadcastPath.value))
      //val all = sortedKmerCounts.sortBy(_._1) //(r => (r._2,r._1),false).take(broadcastN.value)

      //all.saveAsTextFile(output)

      //all.foreach(println)

    }

}