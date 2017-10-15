package skc.multisequence

import multiseq.{DistanceMeasure, SquaredEuclidean}

/**
  * Created by Mara Sorella on 6/26/17.
  */
package object multisequtil {

  object ConfigParameters {
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

  }

  case class MultisequenceTestConfiguration(dataset: String,outputDirectory:String,k: Int, m: Int, x: Int, max_b: Int = 2000,
                               sequenceType:Int=0,canonical:Boolean=true,debug:Boolean=false,write:Boolean=true,useCustomPartitioner:Boolean=false,numPartitionTasks:Int=0,var distanceMeasure:DistanceMeasure = new SquaredEuclidean()){

    val b:Int = Math.min(Math.pow(4,m),max_b).toInt
    val outputDir: String  = outputDirectory + "k" + k + "_m" + m + "_x" + x + "_b" + b+"_s"+sequenceType
    var  testDesc = "Multisequence Kmer counting on Spark. \nTest parameters:\nDataset: "+dataset + "\nk: "+ k + "\nm: " + m + "\nx: " + x + "\nb: " + b +"\nSequence type: "+sequenceType +"\nWriting: "+write+"\nBin Packing Partitioning: "+useCustomPartitioner
    if(useCustomPartitioner)
      testDesc+= "\t no. partition tasks: "+numPartitionTasks

    override def toString: String = testDesc
  }

  case class SequencePair(sorted:Boolean = true)(seq1:String,seq2:String) extends Serializable {
    val(s1,s2) = if(!sorted || (seq1 <= seq2)) (seq1,seq2) else (seq2,seq1)

    override def equals(that:Any) = that match {
      case that: SequencePair => (this.s1 compare that.s1) == 0 && (this.s2 compare that.s2) == 0
      case _ => false
    }

  }




}
