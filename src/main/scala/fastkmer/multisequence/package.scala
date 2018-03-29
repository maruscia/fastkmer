package fastkmer.multisequence

import distances._

/**
  * Created by Mara Sorella on 6/26/17.
  */
package object multisequtil {

  /*object ConfigParameters {
    val KValue = "-k"
    val MValue = "-m"
    val XValue = "-x"
    val BValue = "-B"
    val Write ="-writeCounts"
    val Canonical = "-canonical"
    val CustomPartitioner = "-numTasks"
    val OutputPath = "-outputDir"
    val Input = "-input"
    val UseShortSequences ="-short"
    val UseLongSequences = "-long"

  }*/

  case class MultisequenceTestConfiguration(dataset: String,outputDirectory:String, prefix: String, k: Int, m: Int,
                               sequenceType:Int=0,canonical:Boolean=true,debug:Boolean=false,write:Boolean=true,useCustomPartitioner:Boolean=false,numPartitionTasks:Int=0,var distanceMeasure:DistanceMeasure = new SquaredEuclidean()){

    val outputDir: String  = outputDirectory + prefix + "k" + k + "_m" + m  +"_s"+sequenceType
    var  testDesc = "Multisequence Kmer counting on Spark. \nTest parameters:\nDataset: "+dataset + "\nk: "+ k + "\nm: " + m  +"\nSequence type: "+sequenceType +"\nWriting: "+write+"\nBin Packing Partitioning: "+useCustomPartitioner
    if(useCustomPartitioner)
      testDesc+= "\t no. partition tasks: "+numPartitionTasks

    override def toString: String = testDesc
  }

  def sequenceFactory(seq1:String,seq2:String,sorted:Boolean = true)= {
    if(!sorted || (seq1 <= seq2)) (seq1,seq2) else (seq2,seq1)
  }


}
