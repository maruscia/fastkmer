package skc.multisequence

import multiseq.SquaredEuclidean
import multisequtil._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import skc.SparkBinKmerCounter
import sun.reflect.generics.reflectiveObjects.NotImplementedException


object TestMultisequenceKmerCounter {


  def main(args: Array[String]): Unit = {

    //parse command line arguments
    //val datasetFolder = "hdfs://mycluster/tests/input/"//"hdfs://mycluster/tests/input/"

    //defaults if unspecified
    var k = 20
    var m:Int = 4
    var x = 3
    var b = 2000
    var inputDatasetPath =  ""
    var outputDatasetPath = ""
    var sequenceType = -1
    var useHT = false
    var write = false
    var useCustomPartitioner = false
    var numPartitionTasks = 0

    val it = args.iterator
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


    //Default distance measure: SquaredEuclidean

    val tc = MultisequenceTestConfiguration(inputDatasetPath,outputDatasetPath,k,m,x,max_b=b,sequenceType=sequenceType,write=write,useCustomPartitioner=useCustomPartitioner,numPartitionTasks=numPartitionTasks)
    run(tc)

  }



  def run(configuration: MultisequenceTestConfiguration) {
    val spark = SparkSession
      .builder
      //.config("spark.sql.warehouse.dir", "hdfs:///spark-warehouse")
      //.config("spark.jars", "wasbs:///datasets@grupporisorse13742.blob.core.windows.net/jars/fastutil-7.2.1.jar")
      .appName("SKC Test: k" + configuration.k + " m:" + configuration.m)
      .getOrCreate()

    SparkMultiSequenceKmerCounter.executeJob(spark, configuration)


  }


}