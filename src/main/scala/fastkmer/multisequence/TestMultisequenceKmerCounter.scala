package fastkmer.multisequence

import distances.SquaredEuclidean
import fastkmer.SparkBinKmerCounter
import fastkmer.multisequence.multisequtil.MultisequenceTestConfiguration
import fastkmer.test.testutil.TestConfiguration
import fastkmer.util.Kmer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer



object TestMultisequenceKmerCounter {


  def main(args: Array[String]): Unit = {


    //defaults if unspecified
    var k = 20
    var m:Int = 4
    var x = 3
    var b = 2048

    var inputDatasetPath =  ""
    var outputDatasetPath = ""
    var prefix = ""

    var sequenceType = 0
    var useHT = false
    var write = false
    var useCustomPartitioner = false
    var numPartitionTasks = 0
    var useKryo = false

    k = args(0).toInt
    m = args(1).toInt
    x = args(2).toInt
    useHT =  if(args(3).toInt == 1) true else false
    sequenceType = args(4).toInt
    inputDatasetPath = args(5)
    outputDatasetPath = args(6)
    prefix = args(7)
    write = if(args(8).toInt == 1) true else false
    useKryo = if(args(9).toInt == 1) true else false
    useCustomPartitioner = if(args(10).toInt == 1) true else false
    if(useCustomPartitioner)
      numPartitionTasks = args(11).toInt

    /*val it = args.iterator
    while(it.hasNext) {
      val flagOrParam = it.next()

      if(flagOrParam == ConfigParameters.KValue)
        k=it.next().toInt

      else if(flagOrParam == ConfigParameters.MValue)
        m=it.next().toInt

      else if(flagOrParam == ConfigParameters.XValue)
        x=it.next().toInt

      else if(flagOrParam == ConfigParameters.BValue)
        b=it.next().toInt

      else if(flagOrParam == ConfigParameters.Input)
        inputDatasetPath = it.next()

      else if(flagOrParam == ConfigParameters.OutputPath)
        outputDatasetPath = it.next()

      else if(flagOrParam == ConfigParameters.CustomPartitioner) {
        useCustomPartitioner=true
        numPartitionTasks = it.next().toInt
      }

      else if(flagOrParam == ConfigParameters.Write)
        write = true

      else if(flagOrParam == ConfigParameters.UseShortSequences)
        sequenceType = 0

      else if(flagOrParam == ConfigParameters.UseLongSequences)
        sequenceType = 1

    }

    if(sequenceType == -1)
      throw new NotImplementedError("Must specify sequence type.")
*/

    //Default distance measure: SquaredEuclidean

    val tc = MultisequenceTestConfiguration(inputDatasetPath,outputDatasetPath,k,m,sequenceType=sequenceType,write=write,useCustomPartitioner=useCustomPartitioner,numPartitionTasks=numPartitionTasks)
    run(tc)

  }


  def run(configuration: MultisequenceTestConfiguration) {

    var conf = new SparkConf()
      .setAppName("SKC Test: k" + configuration.k + " m:" + configuration.m)
      .setMaster("local[4]")


    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    SparkMultiSequenceKmerCounter.executeJob(spark, configuration)

  }


}