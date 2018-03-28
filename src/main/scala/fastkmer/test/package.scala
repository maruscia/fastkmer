package fastkmer.test
/**
  * Created by Mara Sorella on 6/14/17.
  */

import org.apache.spark.serializer.KryoRegistrator


package object testutil {

  val debugDirectory = "/tmp/"


  //Case class representing a configuration for a test

  case class TestConfiguration(dataset: String,
                               outputDirectory:String,
                               k: Int,
                               m: Int,
                               x: Int,
                               max_b: Int = 2000,
                               sequenceType:Int=0,
                               canonical:Boolean=true,
                               debug:Boolean=false,
                               write:Boolean=true,
                               useKryoSerializer:Boolean=false,
                               useHT:Boolean=false,
                               useCustomPartitioner:Boolean=false,
                               numPartitionTasks:Int=0,
                               prefix:String = ""){

    val b:Int = Math.min(Math.pow(4,m),max_b).toInt
    val outputDir: String  = if(debug) debugDirectory + prefix + "k" + k + "_m" + m + "_x" + x + "_b" + b else outputDirectory + prefix + "k" + k + "_m" + m + "_x" + x + "_b" + b+"_s"+sequenceType


    //test description
    var  testDesc = "Kmer counting on Spark. \nTest parameters:\nDataset: "+dataset + "\nk: "+ k + "\nm: " + m + "\nx: " + x + "\nb: " + b +"\nSequence type: "+sequenceType +"\nUsing HT:  "+useHT +"\nWriting: "+write+"\nUsing Kryo Serializer: "+useKryoSerializer+"\nMultiprocessor Scheduliong Partitioning: "+useCustomPartitioner
    if(useCustomPartitioner)
      testDesc+= "\t no. partition tasks: "+numPartitionTasks

    override def toString: String = testDesc
  }



}
