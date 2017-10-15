/**
  * Created by Mara Sorella on 5/7/17.
  *
  * */


package skc

import java.io.{BufferedWriter, File, OutputStreamWriter}
import java.net.URI
import java.util.Date
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import util._
import skc.test.testutil._
import fastdoop.{FASTAlongInputFileFormat, FASTAshortInputFileFormat, PartialSequence, Record}
import it.unimi.dsi.fastutil.objects._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.immutable.HashMap
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer


object SparkBinKmerCounter {


  def getSuperKmers(k: Int, m: Int, B: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, _)]): Iterator[(Int, ArrayBuffer[Kmer])] = {
    //debug: start datetime
    val start = new Date(System.currentTimeMillis())
    var t0 = System.nanoTime()/1000000

    def bin(s: Int) = hash_to_bucket(s, B)
    var nreads = 0

    var out = Array.tabulate[(Int, ArrayBuffer[Kmer])](B)(i => (i, new ArrayBuffer[Kmer]))

    var t1 = t0
    var total:Long = 0

    val norm:Array[Int] = fillNorm(m)


    var lastMmask :Long = (1 << m * 2) - 1
    //keeps upper bound on distinct kmers that could be in a bin (for use with extractSuperKmersHT)

    var min_s:Signature = Signature(-1,-1)
    var super_kmer_start = 0
    var s: Kmer = null
    var N_pos = (-1, -1)
    var i = 0

    while (reads.hasNext) {
      val read = reads.next._2

      val cur: Array[Byte] = read match {
        case read: PartialSequence => read.getValue.replaceAll("\n", "").getBytes()
        case read: Record => read.getValue.replaceAll("\n", "").getBytes()
      }

      if (cur.length >= k) {

        min_s = Signature(-1,-1)
        super_kmer_start = 0
        s = null
        N_pos = (-1, -1)
        i = 0

        while (i < cur.length - k + 1) {


          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,i + k) //DEBUG: not much influence


          if (N_pos._1 != -1) {
            //there's at least one 'N'

            if (super_kmer_start < i) {
              // must output a superkmer



              out(bin(min_s.value))._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)

              //println("[out1] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))


            }
            super_kmer_start = i + N_pos._2 + 1 //after last index of N
            i += N_pos._2 + 1
          }
          else {//we have a valid kmer
            s = new Kmer(k,cur,i)


            if (i > min_s.pos) {
              if (super_kmer_start < i) {
                out(bin(min_s.value))._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                //println("[out2] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))

                super_kmer_start = i


              }

              min_s.set(s.getSignature(m,norm),i)

            }
            else {

              val last:Int = s.lastM(lastMmask,norm,m)

              if (last < min_s.value) {

                //add superkmer
                if (super_kmer_start < i) {

                  out(bin(min_s.value))._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                  //println("[out3] "+longToString(min_s.value) + " - " + new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start))

                  super_kmer_start = i


                }

                min_s.set((last,i + k - m))


              }
            }

            i += 1
          }
        }

        if (cur.length - super_kmer_start >= k) {


          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,cur.length)

          if(N_pos._1 == -1){
            out(bin(min_s.value))._2 += new Kmer(cur.length - super_kmer_start, cur, super_kmer_start)
            //println("[out4] " + longToString(min_s.value) + " - " + new Kmer(cur.length - super_kmer_start, cur, super_kmer_start))

          }
          else if(i + N_pos._1 >= super_kmer_start+k){

            out(bin(min_s.value))._2 += new Kmer(i + N_pos._1, cur, super_kmer_start)
            //println("[out5] " + longToString(min_s.value) + " - " + new Kmer(N_pos._1, cur, super_kmer_start))
          }
        }

      }
      nreads += 1
    }

    val end = new Date(System.currentTimeMillis())

    println("Finished getSuperKmers. Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

    //filter empty bins, return iterator
    out.view.filter({case (_,arr) => arr.nonEmpty}).iterator
  }

  //Works on a sample of data to produce estimates on bin sizes, for use with the Partitioner
  def getBinsEstimateSizes(k: Int, m: Int, B: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, _)]): Iterator[(Int, Int)] = {

    val start = new Date(System.currentTimeMillis())
    var t0 = System.nanoTime()/1000000

    def bin(s: Int) = hash_to_bucket(s, B)
    var nreads = 0

    var out = Array.tabulate[(Int, ArrayBuffer[Kmer])](B)(i => (i,  new ArrayBuffer[Kmer]))

    var t1 = t0
    var total:Long = 0

    val norm:Array[Int] = fillNorm(m)

    var lastMmask :Long = (1 << m * 2) - 1
    //keeps upper bound on distinct kmers that could be in a bin
    val binSizes = new Array[Int](B)

    while (reads.hasNext) {

      val read = reads.next._2

           val cur: Array[Byte] = read match {
        case read: PartialSequence => read.getValue.replaceAll("\n", "").getBytes()
        case read: Record => read.getValue.replaceAll("\n", "").getBytes()
      }

      if (cur.length >= k) {
        //initialize vars
        var min_s:Signature = Signature(-1,-1)
        var super_kmer_start = 0
        var s: Kmer = null
        var N_pos = (-1, -1)
        var i = 0
        var bin_no = -1
        while (i < cur.length - k + 1) {



          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,i + k) //DEBUG: not much influence


          if (N_pos._1 != -1) {

            if (super_kmer_start < i) {
              binSizes(bin(min_s.value)) += i - super_kmer_start


            }
            super_kmer_start = i + N_pos._2 + 1
            i += N_pos._2 + 1
          }
          else {
            s = new Kmer(k,cur,i)


            if (i > min_s.pos) {
              if (super_kmer_start < i) {

                binSizes(bin(min_s.value))+= i - super_kmer_start
                super_kmer_start = i

              }
              min_s.set(s.getSignature(m,norm),i)



            }
            else {

              val last:Int = s.lastM(lastMmask,norm,m)
              //println("checking last: "+longToString(last))
              if (last < min_s.value) {

                //add superkmer
                if (super_kmer_start < i) {
                  binSizes(bin(min_s.value))+= i - super_kmer_start

                  super_kmer_start = i


                }

                min_s.set((last,i + k - m))

              }
            }

            i += 1
          }
        }

        if (cur.length - super_kmer_start >= k) {


          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,cur.length)

          if(N_pos._1 == -1){
            binSizes(bin(min_s.value))+=cur.length - super_kmer_start -k +1
          }
          else if(i + N_pos._1 >= super_kmer_start+k){
            binSizes(bin(min_s.value))+= i + N_pos._1 - k + 1

          }
        }

      }
      nreads += 1
    }

    val end = new Date(System.currentTimeMillis())

    println("Finished sample(). Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

    binSizes.zipWithIndex.filter(_._1 >0).map(_.swap).toIterator
  }
  //Generates superkmers and also outputs bin sizes, for use with Hash table
  def getSuperKmersWithBinSizes(k: Int, m: Int, B: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, _)]): Iterator[(Int, (Int, ArrayBuffer[Kmer]))] = {

    val start = new Date(System.currentTimeMillis())
    var t0 = System.nanoTime()/1000000

    def bin(s: Int) = hash_to_bucket(s, B)
    var nreads = 0

    var out = Array.tabulate[(Int, ArrayBuffer[Kmer])](B)(i => (i,  new ArrayBuffer[Kmer]))

    var t1 = t0
    var total:Long = 0

    val norm:Array[Int] = fillNorm(m)

    var lastMmask :Long = (1 << m * 2) - 1

    val binSizes = new Array[Int](B)
    var min_s:Signature = Signature(-1,-1)
    var super_kmer_start = 0
    var s: Kmer = null
    var N_pos = (-1, -1)
    var i = 0
    var bin_no = -1


    while (reads.hasNext) {

      val read = reads.next._2

      val cur: Array[Byte] = read match {
        case read: PartialSequence => read.getValue.replaceAll("\n", "").getBytes()
        case read: Record => read.getValue.replaceAll("\n", "").getBytes()
      }

      if (cur.length >= k) {

        min_s= Signature(-1,-1)
        super_kmer_start = 0
        s= null
        N_pos = (-1, -1)
        i = 0
        bin_no = -1

        while (i < cur.length - k + 1) {

          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,i + k) //DEBUG: not much influence


          if (N_pos._1 != -1) {

            if (super_kmer_start < i) {


              bin_no = bin(min_s.value)

              out(bin_no)._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
              binSizes(bin_no) += i - super_kmer_start

            }
            super_kmer_start = i + N_pos._2 + 1 //after last index of N
            i += N_pos._2 + 1
          }
          else {//we have a valid kmer
            s = new Kmer(k,cur,i)


            if (i > min_s.pos) {
              if (super_kmer_start < i) {

                bin_no = bin(min_s.value)

                out(bin_no)._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                binSizes(bin_no)+= i - super_kmer_start

                super_kmer_start = i


              }
              min_s.set(s.getSignature(m,norm),i)

            }
            else {

              val last:Int = s.lastM(lastMmask,norm,m)

              if (last < min_s.value) {


                if (super_kmer_start < i) {
                  bin_no = bin(min_s.value)
                  out(bin_no)._2 += new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                  binSizes(bin_no)+= i - super_kmer_start

                  super_kmer_start = i


                }

                min_s.set((last,i + k - m))


              }
            }

            i += 1
          }
        }

        if (cur.length - super_kmer_start >= k) {


          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,cur.length)

          if(N_pos._1 == -1){
            bin_no = bin(min_s.value)
            out(bin_no)._2 += new Kmer(cur.length - super_kmer_start, cur, super_kmer_start)
            binSizes(bin_no)+= cur.length - super_kmer_start - k +1

          }
          else if(i + N_pos._1 >= super_kmer_start+k){
            bin_no = bin(min_s.value)
            out(bin(min_s.value))._2 += new Kmer(i + N_pos._1, cur, super_kmer_start)
            binSizes(bin_no)+= i + N_pos._1 - k + 1

          }
        }

      }
      nreads += 1
    }

    val end = new Date(System.currentTimeMillis())

    println("Finished getSuperKmersWithBinSizes(). Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")
    out.view.filter({case (_,arr) => arr.nonEmpty}).map({case (bin:Int,arr:ArrayBuffer[Kmer]) => (bin,(binSizes(bin),arr))}).iterator
  }

  def extractKXmers(k: Int, x: Int, path: String,write:Boolean=true,useKryo:Boolean=false)(bins: Iterator[(Int, ArrayBuffer[Kmer])]): Unit = {

    val start = new Date(System.currentTimeMillis())
    println("["+ start + "] Started extractKXmers()")


    // Array that will contain all (k,x)-mers (R)
    val unsortedR: Array[ArrayBuffer[Kmer]] = Array.fill[ArrayBuffer[Kmer]](x + 1)(new ArrayBuffer[Kmer])
    var sortedR = Array.fill[Array[Kmer]](x + 1)(new Array[Kmer](0))

    var bin: Iterator[Kmer] = null

    var lastOrientation = -1

    var orientation = -1
    var runLength = 0
    var runStart = 0
    var nElementsToSort = 0

    var sk:Kmer = null

    val MB = 1024*1024
    var nBins = 0

    val r = Runtime.getRuntime
    var n:(Int,ArrayBuffer[Kmer]) = null

    while (bins.hasNext) {
      // for each bin

      val binStart = new Date(System.currentTimeMillis())

      n = bins.next

      println("\n["+nBins + "] Examining bin: " + n._1 + " Length: "+n._2.length+"  -  Started: "+binStart+"  [Free memory: " + r.freeMemory()/MB + "MB]")
      bin = n._2.toIterator

      nElementsToSort = 0

      while (bin.hasNext) {
        //for each super-kmer

        sk = bin.next
        //val skCharArr = sk.toCharArray


          lastOrientation = -1
          orientation = -1
          runLength = 0


          // the length of a run is 1 if i have a k-mer
          // 2 if i have a k+1 mer
          // 3 if i have a k+2 mer
          // ...

          for (i <- 0 to sk.length - k) {


            orientation = getOrientation(sk, i, i + k - 1)

            //check if we need to output
            if (orientation == lastOrientation) {

              runLength += 1

              //if we have reached the biggest subsequence possible, must output it
              if (runLength == x + 1) {
                unsortedR(runLength - 1).append(new Kmer(k + runLength - 1, sk, runStart, runStart + k + runLength - 2, orientation))

                nElementsToSort += 1
                runLength = 0
                runStart = i
                lastOrientation = -1
              }
            }

            else {
              //last orientation was different, must output previous sequence
              if (lastOrientation != -1) {
                unsortedR(runLength - 1).append(new Kmer(k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))

                nElementsToSort += 1
              }

              //increase run length
              runLength = 1
              runStart = i
              lastOrientation = orientation
            }

          }
          if (runLength > 0) {
            unsortedR(runLength - 1).append(new Kmer(k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))

            nElementsToSort += 1
          }
        }

        println("built unsorted array [Elapsed: " + getDateDiff(binStart, new Date(System.currentTimeMillis()), TimeUnit.SECONDS) + "s. bin " + n._1)


        for (i <- unsortedR.indices) {
          sortedR(i) = unsortedR(i).toArray
          unsortedR(i).clear()
        }


        println(">> Sorting started (Array of " + nElementsToSort + " total elements. bin " + n._1)

        val befSorting = new Date(System.currentTimeMillis())

        for (i <- sortedR.indices) {
          scala.util.Sorting.quickSort(sortedR(i))
        }

        println(">> Sorting finished -, Elapsed: " + getDateDiff(befSorting, new Date(System.currentTimeMillis()), TimeUnit.SECONDS) + "s. bin " + n._1)
        val heap = priorityQueueWithIndexes(sortedR, k)

        val befHeap = new Date(System.currentTimeMillis())
        if (heap.nonEmpty) {

          val outputPath = path + "/bin" + n._1

          lazy val outputStream = FileSystem.get(URI.create(outputPath), new Configuration()).create(new Path(outputPath))

          lazy val writer = new BufferedWriter(new OutputStreamWriter(outputStream))

          //KryoSerializer (if useKryo is true)
          lazy val output = new Output(FileSystem.get(URI.create(outputPath), new Configuration()).create(new Path(outputPath)))
          lazy val kryo = new Kryo()

          var index: RIndex = null
          var last_kmer: Kmer = null
          var last_kmer_cnt = 0

          println(">>> Started outputting k-mer counts for bin " + n._1)

          while (heap.nonEmpty) {
            index = heap.dequeue()

            //println("Found kmer: "+index.pointedKmer)
            //println("Last kmer: "+last_kmer)
            if (index.pointedKmer == last_kmer) {
              //println("Incrementing counter.")
              last_kmer_cnt += 1
            }
            else {
              if (last_kmer != null) {

                //kmer has changed, send out previous one with completed count
                if (write)
                  if (useKryo) kryo.writeObject(output, last_kmer + "\t" + last_kmer_cnt + '\n')
                  else writer.write(last_kmer + "\t" + last_kmer_cnt + '\n')

                //println("[W] "+last_kmer + " " +last_kmer_cnt)

              }
              last_kmer = index.pointedKmer
              last_kmer_cnt = 1
            }
            index.advance()

            if (!index.exhausted) {//if it still has kmers to read

              heap.enqueue(index) //put it back in the heap

              //println("enqueuing: "+index.pointedKmer)
            }
          }
          if (write) {
            if (useKryo) {
              kryo.writeObject(output, last_kmer + "\t" + last_kmer_cnt + '\n')
              kryo.writeObject(output, "EOF")
              output.close()
            }
            else {
              writer.write(last_kmer + "\t" + last_kmer_cnt + '\n')
              writer.write("EOF")

              /*
              DEBUG code: to be used for checking which are the failing bins(if any)
              TODO: create B files in filenamePath (not HDFS!), e.g. with a command like:
                  printf '%s ' {0..2047} | xargs touch


              val conf = new Configuration()
              conf.set("fs.defaultFS", "file:///afs/sta.uniroma1.it/user/u/uferraro/")
              val localFs = FileSystem.get(conf)

              val binsPath = new org.apache.hadoop.fs.Path("file:///afs/sta.uniroma1.it/user/u/uferraro/bin_success/"+n._1)
              val donePath = new org.apache.hadoop.fs.Path("file:///afs/sta.uniroma1.it/user/u/uferraro/bin_success/done/"+n._1)
              print("Deleting " +n._1+" .... ")

              if (localFs. exists(binsPath)) {
                localFs.delete(binsPath, true)

                println("OK!")

                val o = localFs.create(donePath)
                val w = new BufferedWriter(new OutputStreamWriter(o))
                w.write("SUCCESS\n")
                w.close()


              }
              else{
                println("file does not exist. Yet I can list:\n")
                val status = localFs.listStatus(new Path("file:///afs/sta.uniroma1.it/user/u/uferraro/bin_success/dir"))
                status.foreach(x=> println(x.getPath))
              }
                */

              writer.close()
            }


          }

        }
        println(">>> Finished outputting - [Free memory: " + r.freeMemory() / MB + "MB], Elapsed: " + getDateDiff(befHeap, new Date(System.currentTimeMillis()), TimeUnit.SECONDS) + "s. bin" + n._1)

        for (i <- sortedR.indices) {
          sortedR(i) = null
        }

      println("End processing bin " + n._1 + ". Total elapsed time: "+ getDateDiff(binStart,new Date(System.currentTimeMillis()),TimeUnit.SECONDS) +"s")
      nBins+=1

      }
    val end = new Date(System.currentTimeMillis())
    println("extractKXmers ended. Elapsed: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")
  }


// extracts Kxmers (Hash table)
  def extractKXmersHT(k: Int, x: Int, path: String,write:Boolean=true,useKryo:Boolean=false)(bins: Iterator[(Int, (Int,ArrayBuffer[Kmer]))]): Unit = {
    val start = new Date(System.currentTimeMillis())
    println("[" + start + "] Started extractKXmersHT() with path "+path  +"writing: "+write)
    var nBins: Int = 0
    val MB = 1024 * 1024
    var bin: Iterator[Kmer] = null
    var kmersUpperBoundForBin: Int = 0

    var sk: Kmer = null
    var orientation = 0
    val r = Runtime.getRuntime
    //instantiate map with upper bound of kmers
    var map:Object2IntOpenHashMap[Kmer] = null


    var kmer: Kmer = null

    while (bins.hasNext) {
      // for each bin

      val binStart = new Date(System.currentTimeMillis())

      val n: (Int, (Int,ArrayBuffer[Kmer])) = bins.next
      bin = n._2._2.toIterator
      kmersUpperBoundForBin = n._2._1
      println("\n[" + nBins + "] Examining bin: " + n._1 + " size: "+kmersUpperBoundForBin+" -  [Free memory: " + r.freeMemory() / MB + "MB]")


      map = new Object2IntOpenHashMap[Kmer](kmersUpperBoundForBin)

      while (bin.hasNext) {
        //for each super-kmer
        sk = bin.next

        for (i <- 0 to sk.length - k) {

          orientation = getOrientation(sk, i, i + k - 1)
          kmer = new Kmer(k, sk, i, i + k - 1, orientation)

          map.addTo(kmer,1)

        }


      }


      if (map.size()>0) {



        lazy val outputPath = path + "/bin" + n._1
        lazy val writer = new BufferedWriter(new OutputStreamWriter(FileSystem.get(URI.create(outputPath),new Configuration()).create(new Path(outputPath))))

        //KryoSerializer (if useKryo is true)
        lazy val output = new Output(FileSystem.get(URI.create(outputPath),new Configuration()).create(new Path(outputPath)))
        lazy val kryo = new Kryo()


        val it = map.object2IntEntrySet().fastIterator()

        while (it.hasNext) {
          val km = it.next()
          if(write)
            if(useKryo) kryo.writeObject(output,km.getKey + "\t" + "\t" + km.getIntValue + '\n')
            else writer.write(km.getKey + "\t" + km.getIntValue + '\n')
        }

        if(write)
          if(useKryo) output.close()
          else writer.close()
      }


    }
  }
  //debug task to evaluate balance of partitions

  def evaluatePartitionBalance(path:String)(bins: Iterator[(Int, ArrayBuffer[Kmer])]): Unit = {
    val partitionId = TaskContext.get.partitionId

    val arr = ArrayBuffer[(Int, Int)]()
    var outputFilename = "/balance_"+partitionId

    var first = true

    while (bins.hasNext) {

      val b: (Int, ArrayBuffer[Kmer]) = bins.next
      arr.append((b._1, b._2.size))

      println("Key: " + b._1 + " - size: " + b._2.size)
    }

    val outputStream = FileSystem.get(URI.create("hdfs://mycluster"),new Configuration()).create(new Path(path + outputFilename))
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    var total = 0
    for(a <- arr.sortBy(_._2)){
      writer.write(a._1 + "," + a._2 + '\n')
      total += a._2
    }
    writer.write("Total: " +total + '\n')
    writer.close()
  }


  //debug Job to determine which signatures are sent to each bin

  def getBinSignatures(k: Int, m: Int, B: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, _)]): Iterator[(Int, immutable.HashMap[String,Int])] = {
    //debug: start datetime
    val start = new Date(System.currentTimeMillis())
    var t0 = System.nanoTime()/1000000

    def bin(s: Int) = hash_to_bucket(s, B)
    var nreads = 0



    var out = Array.tabulate[(Int, (mutable.HashMap[String,Int]))](B)(i => (i, new mutable.HashMap[String,Int]))

    var t1 = t0
    var total:Long = 0

    val norm:Array[Int] = fillNorm(m)


    var lastMmask :Long = (1 << m * 2) - 1
    //keeps upper bound on distinct kmers that could be in a bin (for use with extractSuperKmersHT)

    var min_s:Signature = Signature(-1,-1)
    var super_kmer_start = 0
    var s: Kmer = null
    var N_pos = (-1, -1)
    var i = 0



    while (reads.hasNext) {
      val read = reads.next._2


      val cur: Array[Byte] = read match {
        case read: PartialSequence => read.getValue.replaceAll("\n", "").getBytes()
        case read: Record => read.getValue.replaceAll("\n", "").getBytes()
      }

      if (cur.length >= k) {

        min_s = Signature(-1,-1)
        super_kmer_start = 0
        s = null
        N_pos = (-1, -1)
        i = 0

        while (i < cur.length - k + 1) {

          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,i + k) //DEBUG: not much influence


          if (N_pos._1 != -1) {
            //there's at least one 'N'

            if (super_kmer_start < i) {
              // must output a superkmer
              val outBin = bin(min_s.value)
              val sig = longToString(min_s.value,m)
              out(outBin)._2.update(sig, out(outBin)._2.getOrElse(sig, 0) + 1)



            }
            super_kmer_start = i + N_pos._2 + 1 //after last index of N
            i += N_pos._2 + 1
          }
          else {//we have a valid kmer
            s = new Kmer(k,cur,i)


            if (i > min_s.pos) {
              if (super_kmer_start < i) {
                val outBin = bin(min_s.value)
                val sig = longToString(min_s.value,m)
                out(outBin)._2.update(sig, out(outBin)._2.getOrElse(sig, 0) + 1)


                super_kmer_start = i


              }

              min_s.set(s.getSignature(m,norm),i)



            }
            else {

              val last:Int = s.lastM(lastMmask,norm,m)

              if (last < min_s.value) {


                if (super_kmer_start < i) {

                  val outBin = bin(min_s.value)
                  val sig = longToString(min_s.value,m)
                  out(outBin)._2.update(sig, out(outBin)._2.getOrElse(sig, 0) + 1)

                  super_kmer_start = i

                }

                min_s.set((last,i + k - m))


              }
            }

            i += 1
          }
        }

        if (cur.length - super_kmer_start >= k) {

          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,cur.length)

          val outBin = bin(min_s.value)
          val sig = longToString(min_s.value,m)
          if(N_pos._1 == -1){

            out(outBin)._2.update(sig, out(outBin)._2.getOrElse(sig, 0) + 1)

          }
          else if(i + N_pos._1 >= super_kmer_start+k){

            out(outBin)._2.update(sig, out(outBin)._2.getOrElse(sig, 0) + 1)


          }

        }

      }
      nreads += 1
    }

    val end = new Date(System.currentTimeMillis())

    type HM[_] = HashMap[String, Int]

    println("Finished getSignaturesInBins. Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

    out.view.filter({case (_,map) => map.nonEmpty}).map({case (integer,map:mutable.HashMap[String,Int])=> (integer,map.to[HM])}).iterator
  }


  def saveBinSignatures(path: String)(bins: Iterator[(Int, immutable.HashMap[String,Int])]): Unit = {
    val start = new Date(System.currentTimeMillis())
    println("[" + start + "] Started saveBinSignatures() with path "+path)

    var bin: Iterator[String] = null

    while (bins.hasNext) {
      // for each bin


      val n: (Int,immutable.HashMap[String,Int]) = bins.next
      val it = n._2.iterator
      lazy val outputPath = path + "/bin_signatures" + n._1 +".txt"
      lazy val writer = new BufferedWriter(new OutputStreamWriter(FileSystem.get(URI.create(outputPath),new Configuration()).create(new Path(outputPath))))
      var tot = 0
      var totSigs = 0
      while (it.hasNext){

          val km = it.next()

          writer.write(km._1 + "\t" + km._2 + '\n')
          tot+=km._2
          totSigs+=1
        }
      writer.write("Total\t" + tot + '\n')
      println("[BIN " +n._1 +"] Total # signatures: " +totSigs+ " Total superkmers from signatures " +tot)
      writer.close()
      }

    val end = new Date(System.currentTimeMillis())
    println("[" + end + "] saveBinSignatures() ended.")


  }

//debug job to save which signatures are sent to each bin
  def executeFindBinSignaturesJob(spark: SparkSession, configuration: TestConfiguration):Unit = {
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", configuration.k.toString)


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
      if (configuration.sequenceType == 0)
        sc.newAPIHadoopFile(FASTfile, classOf[FASTAshortInputFileFormat], classOf[NullWritable], classOf[fastdoop.Record], conf)
      else sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)
    val binSigs = sequencesRDD.mapPartitions(getBinSignatures(broadcastK.value, broadcastM.value, broadcastB.value, broadcastC.value))
    type HM[_] = HashMap[String, Int]

    binSigs.reduceByKey((map1,map2) => (map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }).to[HM]).foreachPartition(saveBinSignatures(broadcastPath.value))

  }


  def executeJob(spark: SparkSession, configuration: TestConfiguration): Unit = {

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("k", configuration.k.toString)

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
      if (configuration.sequenceType == 0)
        sc.newAPIHadoopFile(FASTfile, classOf[FASTAshortInputFileFormat], classOf[NullWritable], classOf[fastdoop.Record], conf)
      else sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)


    var sortedbinSizeEstimate:Array[(Int,Int)] = null
    var partitioner:MultiprocessorSchedulingPartitioner = null

    //println("collected: "+ sortedbinSizeEstimate.mkString(", "))

    //readPartitions.foreachPartition(evaluatePartitionBalance(broadcastPath.value))

    //USING PARTITIONER? if so, need to get estimate bin sizes on a sample as a first, preliminary stage.
    if(configuration.useCustomPartitioner){
      sortedbinSizeEstimate = sequencesRDD.sample(withReplacement=false,fraction=0.01).mapPartitions(getBinsEstimateSizes(broadcastK.value, broadcastM.value, broadcastB.value, broadcastC.value)).reduceByKey(_+_).sortBy(_._2, ascending = false).collect()
      partitioner = new MultiprocessorSchedulingPartitioner(configuration.numPartitionTasks,sortedbinSizeEstimate)
    }

    //then,

    //USING HASH TABLE?
    if (!configuration.useHT) {//no
      val superKmers = sequencesRDD.mapPartitions(getSuperKmers(broadcastK.value, broadcastM.value, broadcastB.value, broadcastC.value))
      if(configuration.useCustomPartitioner)
        superKmers.partitionBy(partitioner).reduceByKey(_ ++ _).foreachPartition(extractKXmers(broadcastK.value, broadcastX.value, broadcastPath.value,write = configuration.write,useKryo=configuration.useKryoSerializer)) //mapPartitions(extractKXmers(broadcastK.value, broadcastX.value)) //.mapPartitions(_.toList.sortBy(r => (r._2,r._1)).takeRight(broadcastN.value).toIterator)
      else superKmers.reduceByKey(_ ++ _).foreachPartition(extractKXmers(broadcastK.value, broadcastX.value, broadcastPath.value,write = configuration.write,useKryo=configuration.useKryoSerializer))
    }
    else {//yes
      val superKmers = sequencesRDD.mapPartitions(getSuperKmersWithBinSizes(broadcastK.value, broadcastM.value, broadcastB.value, broadcastC.value))
      if (configuration.useCustomPartitioner)
        superKmers.partitionBy(partitioner).reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2)).foreachPartition(extractKXmersHT(broadcastK.value, broadcastX.value, broadcastPath.value, write = configuration.write,useKryo = configuration.useKryoSerializer))

      else superKmers.reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2)).foreachPartition(extractKXmersHT(broadcastK.value, broadcastX.value, broadcastPath.value, write = configuration.write,useKryo = configuration.useKryoSerializer))

    }

  }


}
