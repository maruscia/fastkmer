package fastkmer.multisequence

/**
  * Created by Mara Sorella on 6/14/17.
  */

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.util.Date
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import fastdoop.{FASTAlongInputFileFormat, FASTAshortInputFileFormat, PartialSequence, Record}
import distances._
import fastkmer.MultiprocessorSchedulingPartitioner
import fastkmer.multisequence.multisequtil.{MultisequenceTestConfiguration, SequencePair}
import fastkmer.util._
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object SparkMultiSequenceKmerCounter {

  def getSuperKmers(k: Int, m: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, _)]):  Iterator[(Int, (String,ArrayBuffer[Kmer]))] = {
    //debug: start datetime
    val start = new Date(System.currentTimeMillis())
    var t0 = System.nanoTime()/1000000

    var nreads = 0
    var out = new SuperkmerHashMap[Int,ArrayBuffer[Kmer]](Math.pow(3,m).toInt + 1)

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
    var new_kmer:Kmer = null

    //sequence name
    var sequence: String = null


    while (reads.hasNext) {
      val read = reads.next._2
      val cur: Array[Byte] = read match {
        case read: PartialSequence => if(sequence == null) sequence ="(\\w+).".r.findFirstIn(read.getKey).getOrElse(""); read.getValue.replaceAll("\n", "").getBytes()
        case read: Record => if(sequence == null) sequence = "(\\w+).".r.findFirstIn(read.getKey).getOrElse(""); read.getValue.replaceAll("\n", "").getBytes()
      }

      if (cur.length >= k) {

        min_s = Signature(-1,-1)
        super_kmer_start = 0
        s = null
        N_pos = (-1, -1)
        i = 0

        while (i < cur.length - k + 1) {


          N_pos = firstAndLastOccurrenceOfInvalidNucleotide('N',cur,i,i + k)


          if (N_pos._1 != -1) {


            if (super_kmer_start < i) {
              // must output a superkmer
              new_kmer = new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
              if(out.contains(min_s.value)){
                out(min_s.value).append(new_kmer)
              }
              else{
                out(min_s.value)= ArrayBuffer[Kmer](new_kmer)
              }

            }
            super_kmer_start = i + N_pos._2 + 1 //after last index of N
            i += N_pos._2 + 1
          }
          else {//we have a valid kmer
            s = new Kmer(k,cur,i)


            if (i > min_s.pos) {
              if (super_kmer_start < i) {
                new_kmer=new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                if(out.contains(min_s.value)){
                  out(min_s.value).append(new_kmer)
                }
                else{
                  out(min_s.value)= ArrayBuffer[Kmer](new_kmer)
                }

                super_kmer_start = i


              }

              min_s.set(s.getSignature(m,norm),i)

            }
            else {

              val last:Int = s.lastM(lastMmask,norm,m)

              if (last < min_s.value) {

                //add superkmer
                if (super_kmer_start < i) {
                  new_kmer = new Kmer(i - 1 + k - super_kmer_start,cur,super_kmer_start)
                  if(out.contains(min_s.value)){
                    out(min_s.value).append(new_kmer)
                  }
                  else{
                    out(min_s.value)= ArrayBuffer[Kmer](new_kmer)
                  }

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
            new_kmer=new Kmer(cur.length - super_kmer_start, cur, super_kmer_start)

            if(out.contains(min_s.value)){
              out(min_s.value).append(new_kmer)
            }
            else{
              out(min_s.value)= ArrayBuffer[Kmer](new_kmer)
            }

          }
          else if(i + N_pos._1 >= super_kmer_start+k){
            new_kmer=new Kmer(i + N_pos._1, cur, super_kmer_start)
            if(out.contains(min_s.value)){
              out(min_s.value).append(new_kmer)
            }
            else{
              out(min_s.value)= ArrayBuffer[Kmer](new_kmer)
            }

          }
        }

      }
      nreads += 1
    }

    val end = new Date(System.currentTimeMillis())

    println("Finished getSuperKmersSignatures. Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

    out.view.map{ case (x,y)=> (x,(sequence,y))}.iterator
  }


  def getBinsEstimateSizes(k: Int, m: Int, B: Int, bothStrands: Boolean)(reads: Iterator[(NullWritable, _)]): Iterator[(Int, Int)] = {
      //debug: start datetime
      val start = new Date(System.currentTimeMillis())
      var t0 = System.nanoTime()/1000000

      def bin(s: Int) = hash_to_bucket(s, B)
      var nreads = 0

      var out = Array.tabulate[(Int, ArrayBuffer[Kmer])](B)(i => (i,  new ArrayBuffer[Kmer]))

      var t1 = t0
      var total:Long = 0

      val norm:Array[Int] = fillNorm(m)

      var lastMmask :Long = (1 << m * 2) - 1
      //keeps upper bound on distinct kmers that could be in a bin (for use with extractSuperKmersHT)
      val binSizes = new Array[Int](B)

      while (reads.hasNext) {

        val read = reads.next._2

        //println(read)

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
              //there's at least one 'N'

              if (super_kmer_start < i) {
                // must output a superkmer

                binSizes(bin(min_s.value)) += i - super_kmer_start


              }
              super_kmer_start = i + N_pos._2 + 1 //after last index of N
              i += N_pos._2 + 1
            }
            else {//we have a valid kmer
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


      //filter empty bins, return iterator
      //out.iterator.foreach(println)

      println("Finished sample(). Total time: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

      binSizes.zipWithIndex.filter(_._1 >0).map(_.swap).toIterator
    }

/*deprecated: for previous FastKmer implementation
    def extractKXmersAndComputePartialDistances(k: Int, x: Int, path: String, write:Boolean=true, distanceMeasure: DistanceMeasure)(bins: Iterator[(Int, ArrayBuffer[(String,ArrayBuffer[Kmer])])]): Iterator[(SequencePair,Double)] = {
      //debug: start datetime

      val start = new Date(System.currentTimeMillis())
      println("["+ start + "] Started extractKXmers()")


      // Array that will contain all (k,x)-mers (R)
      val unsortedR: Array[ArrayBuffer[Kmer]] = Array.fill[ArrayBuffer[Kmer]](x + 1)(new ArrayBuffer[Kmer])
      var sortedR = Array.fill[Array[Kmer]](x + 1)(new Array[Kmer](0))

      var sequenceBin: Iterator[Kmer] = null


      var lastOrientation = -1

      var orientation = -1
      var runLength = 0
      var runStart = 0
      var nElementsToSort = 0

      var sk:Kmer = null

      var nBins = 0
      val sequenceSimilarities = new mutable.HashMap[SequencePair,Double]()

      var binNumber = -1

      var sequenceNames = Seq[String]()

      while (bins.hasNext) {
        // for each bin

        val binStart = new Date(System.currentTimeMillis())
        val sequencesInBin = bins.next() //Int, ArrayBuffer[(String,ArrayBuffer[Kmer])]



        binNumber = sequencesInBin._1
        println("\n["+nBins + "] Examining bin: " + binNumber)


        for((sequenceName,sequenceArr) <- sequencesInBin._2) {
          val seqId:Short = if (!sequenceNames.contains(sequenceName)) {
            sequenceNames = sequenceNames :+ sequenceName

            (sequenceNames.length -1).toShort
          }
          else sequenceNames.indexOf(sequenceName).toShort


          sequenceBin = sequenceArr.toIterator

          nElementsToSort = 0

          while (sequenceBin.hasNext) {
            //for each super-kmer

            sk = sequenceBin.next

            lastOrientation = -1
            orientation = -1
            runLength = 0


            // the length of a run is 1 if i have a k-mer
            // 2 if i have a k+1 mer
            // 3 if i have a k+2 mer
            // ...

            for (i <- 0 to sk.length - k) {


              orientation = getOrientation(sk, i, i + k - 1) //getOrientation(skCharArr,i,i+k-1)

              //check if we need to output
              if (orientation == lastOrientation) {

                runLength += 1

                //if we have reached the biggest subsequence possible, must output it
                if (runLength == x + 1) {

                  unsortedR(runLength - 1).append(new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, orientation))
                  //println("adding: "+new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, orientation))
                  nElementsToSort += 1
                  runLength = 0
                  runStart = i
                  lastOrientation = -1
                }
              }

              else {
                //last orientation was different, must output previous sequence
                if (lastOrientation != -1) {
                  unsortedR(runLength - 1).append(new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
                  //println("adding: "+new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
                  nElementsToSort += 1
                }

                //increase run length
                runLength = 1
                runStart = i
                lastOrientation = orientation
              }

            }
            if (runLength > 0) {
              unsortedR(runLength - 1).append(new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
              //println("adding: "+new KmerWithSequence(seqId,k + runLength - 1, sk, runStart, runStart + k + runLength - 2, lastOrientation))
              nElementsToSort += 1
            }
          }

        }//END FOREACH SEQUENCE IN BIN

        println("built unsorted array [Elapsed: " + getDateDiff(binStart,new Date(System.currentTimeMillis()),TimeUnit.SECONDS) +"s")


        for (i <- unsortedR.indices) {
          sortedR(i) = unsortedR(i).toArray
          unsortedR(i).clear()
        }

        //println(">> Size of sortedR Array is :" +estimateSize(sortedR))
        println(">> Sorting started (Array of " + nElementsToSort + " total elements.")

        val befSorting = new Date(System.currentTimeMillis())

        for (i <- sortedR.indices){
          scala.util.Sorting.quickSort(sortedR(i))
        }

        println(">> Sorting finished -, Elapsed: " + getDateDiff(befSorting,new Date(System.currentTimeMillis()),TimeUnit.SECONDS) +"s")
        val heap = priorityQueueWithIndexes(sortedR, k)

        val befHeap = new Date(System.currentTimeMillis())
        if (heap.nonEmpty) {

          val outputPath = path + "/bin" + binNumber

          lazy val outputStream = FileSystem.get(URI.create("/Users/maru/Documents/"),new Configuration()).create(new Path(outputPath))//hdfs://mycluster
          lazy val writer = new BufferedWriter(new OutputStreamWriter(outputStream))

          var index: RIndex = null
          var last_kmer: Kmer = null
          var last_kmer_seq_counts = new Array[Int](sequenceNames.length)

          println(">>> Started outputting k-mer counts")

          //initialize sequence similarities
          if(sequenceSimilarities.isEmpty)
            for(s1 <- sequenceNames.indices)
              for (s2  <- s1+1 to sequenceNames.length)
                  sequenceSimilarities.put(SequencePair()(sequenceNames(s1),sequenceNames(s2)),distanceMeasure.initDistance())



          while (heap.nonEmpty) {
            index = heap.dequeue()




            if (index.pointedKmer == last_kmer) {
              //increment counter for sequence
              last_kmer_seq_counts(index.pointedKmer.asInstanceOf[KmerWithSequence].getSequence) += 1

            }
            else {
              if (last_kmer != null) {

                //kmer has changed
                // 1. update sequence similarity with info from last seen kmer
                for(s1 <- sequenceNames.indices)
                  for (s2  <- s1+1 to sequenceNames.length){
                    val pair = SequencePair()(sequenceNames(s1),sequenceNames(s2))
                    sequenceSimilarities.put(pair,distanceMeasure.distanceOperator(
                      sequenceSimilarities.get(pair).asInstanceOf[Double],
                      distanceMeasure.computePartialDistance(new Parameters(last_kmer_seq_counts(s1),last_kmer_seq_counts(s2))))
                    )

                  }


                if(write) {
                  //write sum of counts
                  writer.write(last_kmer + "\t" + last_kmer_seq_counts.sum + "\n")
                }
                //reset count
                for(seqIndex <- last_kmer_seq_counts.indices) last_kmer_seq_counts(seqIndex)=0

                }
                //println("[W] "+last_kmer + " " +last_kmer_cnt)



              last_kmer = index.pointedKmer
              last_kmer_seq_counts(last_kmer.asInstanceOf[KmerWithSequence].getSequence) = 1
            }
            index.advance()

            if (!index.exhausted) {
              //if it still has kmers to read
              heap.enqueue(index) //put it back in the heap

            }


          }
          //update info for final kmer, and optionally write

          for(s1 <- sequenceNames.indices)
            for (s2  <- s1+1 to sequenceNames.length){
              val pair = SequencePair()(sequenceNames(s1),sequenceNames(s2))
              sequenceSimilarities.put(pair,distanceMeasure.distanceOperator(
                sequenceSimilarities.get(pair).asInstanceOf[Double],
                distanceMeasure.computePartialDistance(new Parameters(last_kmer_seq_counts(s1),last_kmer_seq_counts(s2))))
              )

            }

          //last kmer write count, and close
            if(write)
              writer.write(last_kmer + "\t" + last_kmer_seq_counts.sum + "\n")
              writer.close()

        }

        println(">>> Finished outputting -  Elapsed: "+getDateDiff(befHeap,new Date(System.currentTimeMillis()),TimeUnit.SECONDS) +"s")


        for (i <- sortedR.indices) {
          sortedR(i) = null
        }


        println("End processing bin. Total elapsed time: "+ getDateDiff(binStart,new Date(System.currentTimeMillis()),TimeUnit.SECONDS) +"s")
        nBins+=1
      }



      val end = new Date(System.currentTimeMillis())
      println("extractKXmers ended. Elapsed: "+ getDateDiff(start,end,TimeUnit.SECONDS) +"s")

      sequenceSimilarities.toIterator
    }
*/
  //Second phase kmer writing implementation (ignore the fields outputPartition (for use with custom partitioner) and Kryo (custom serializer)
  def extractKXmersHTAndComputePartialDistances(k: Int, path: String,write:Boolean=true,useKryo:Boolean=false,outputPartition:Boolean=false,distanceMeasure: DistanceMeasure)(bins: Iterator[(Int, ArrayBuffer[(String,ArrayBuffer[Kmer])])]): Iterator[(SequencePair,Double)] = {

    val start = new Date(System.currentTimeMillis())
    println("[" + start + "] Started extractKXmersHTAndComputePartialDistances() with path " + path + "writing: " + write)

    var nBins: Int = 0
    var binNumber = -1

    var sequenceBin: Iterator[Kmer] = null
    var kmersUpperBoundForBin: Int = 0

    var sk: Kmer = null
    var orientation = 0
    val r = Runtime.getRuntime

    var map: Object2IntOpenHashMap[Kmer] = null


    var kmer: Kmer = null


    //output
    var outputPath: String = null
    var outputStream: FSDataOutputStream = null
    var writer: BufferedWriter = null
    var output: Output = null
    var kryo: Kryo = null

    //Sequences related variables
    val sequenceSimilarities = new mutable.HashMap[SequencePair, Double]() //holding partial similarity values for each pair of seqs

    var sequenceNames = Seq[String]() // holding the


    if (write && outputPartition) {
      print("Processing partition: " + TaskContext.getPartitionId())
      outputPath = path + "/partition" + TaskContext.getPartitionId()
      outputStream = FileSystem.get(URI.create(outputPath), new Configuration()).create(new Path(outputPath))
      writer = new BufferedWriter(new OutputStreamWriter(outputStream))
      output = new Output(FileSystem.get(URI.create(outputPath), new Configuration()).create(new Path(outputPath)))
      kryo = new Kryo()
    }

    while (bins.hasNext) {
      // for each bin

      val binStart = new Date(System.currentTimeMillis())
      val sequencesInBin = bins.next() //Int, ArrayBuffer[(String,ArrayBuffer[Kmer])]

      binNumber = sequencesInBin._1
      println("\n[" + nBins + "] Examining bin: " + binNumber)


      for ((sequenceName, sequenceArr) <- sequencesInBin._2) {
        val seqId: Short = if (!sequenceNames.contains(sequenceName)) {
          sequenceNames = sequenceNames :+ sequenceName

          (sequenceNames.length - 1).toShort
        }
        else sequenceNames.indexOf(sequenceName).toShort


        sequenceBin = sequenceArr.toIterator

        while (sequenceBin.hasNext) {
          //for each super-kmer

          sk = sequenceBin.next

          for (i <- 0 to sk.length - k) {

            orientation = getOrientation(sk, i, i + k - 1)
            kmer = new Kmer(k, sk, i, i + k - 1, orientation)

            map.addTo(kmer, 1)

            //Here you should use a map-like datastructure that takes into account separately, the counts for each sequence
            //e.g.,:
            //Key: Kmer   Value Seq[(seq1,count1),(seq2,count2),...)]


          }
        }

      }

      //Initialize sequence similarities: depending on how you handle sequences, you might use something similar
      if (sequenceSimilarities.isEmpty)
        for (s1 <- sequenceNames.indices)
          for (s2 <- s1 + 1 to sequenceNames.length)
            sequenceSimilarities.put(SequencePair()(sequenceNames(s1), sequenceNames(s2)), distanceMeasure.initDistance())

      if (map.size() > 0) {


        if (!outputPartition && write) {
          outputPath = path + "/bin" + binNumber
          if (useKryo) {
            kryo = new Kryo()
            output = new Output(FileSystem.get(URI.create(outputPath), new Configuration()).create(new Path(outputPath)))
          }
          else
            writer = new BufferedWriter(new OutputStreamWriter(FileSystem.get(URI.create(outputPath), new Configuration()).create(new Path(outputPath))))
        }

        val it = map.object2IntEntrySet().fastIterator()

        while (it.hasNext) {
          val km = it.next()

          // 1. Here you should update the sequence similarities with this kmer
          for (s1 <- sequenceNames.indices)
            for (s2 <- s1 + 1 to sequenceNames.length) {
              val pair = SequencePair()(sequenceNames(s1), sequenceNames(s2))
              sequenceSimilarities.put(pair, distanceMeasure.distanceOperator(
                sequenceSimilarities.get(pair).asInstanceOf[Double],
                distanceMeasure.computePartialDistance(new Parameters(//counts(sequence1),counts(sequence2) <- you should get them from your map
                )))
              )

            }

          if (write)
            if (useKryo) kryo.writeObject(output, km.getKey + "\t" + "\t" + km.getIntValue + '\n')
            else writer.write(km.getKey + "\t" + km.getIntValue + '\n')
        }

        if (write && !outputPartition) {
          if (useKryo) output.close()
          else writer.close()
        }
      }


    }

    if(outputPartition && write){
      if(useKryo) output.close()
      else writer.close()
    }

    //you should probably output to next phase for aggregation
    sequenceSimilarities.iterator
  }


  @throws(classOf[UnsupportedOperationException]) def executeJob(spark: SparkSession, configuration: MultisequenceTestConfiguration): Unit = {

      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      conf.set("k", configuration.k.toString)

      val FASTfile = configuration.dataset

      println(this.getClass.getSimpleName)
      println(configuration)

      val broadcastK = sc.broadcast(configuration.k)
      //val broadcastX = sc.broadcast(configuration.x)
      val broadcastM = sc.broadcast(configuration.m)
      val broadcastC = sc.broadcast(configuration.canonical)

      val broadcastPath = sc.broadcast(configuration.outputDir)

      //FASTQ: sc.newAPIHadoopFile(FASTQfile, classOf[FASTQInputFileFormat], classOf[NullWritable], classOf[QRecord]), conf)

      val sequencesRDD =
        if (configuration.sequenceType == 0)
          sc.newAPIHadoopFile(FASTfile, classOf[FASTAshortInputFileFormat], classOf[NullWritable], classOf[fastdoop.Record], conf)
        else sc.newAPIHadoopFile(FASTfile, classOf[FASTAlongInputFileFormat], classOf[NullWritable], classOf[PartialSequence], conf)

        val superKmers = sequencesRDD.mapPartitions(getSuperKmers(broadcastK.value, broadcastM.value, broadcastC.value))
        superKmers.aggregateByKey(new ArrayBuffer[(String,ArrayBuffer[Kmer])])((accum,el)=> accum += el,_ ++ _).mapPartitions(extractKXmersHTAndComputePartialDistances(broadcastK.value, broadcastPath.value,write = configuration.write,distanceMeasure = configuration.distanceMeasure))
    }



  }
