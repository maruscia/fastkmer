package fastkmer.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import fastkmer.SparkBinKmerCounter
import fastkmer.test.testutil.TestConfiguration
import fastkmer.util.Kmer

import scala.collection.mutable.ArrayBuffer


object LocalTestKmerCounter {

//This object is to be used for tests in local mode


  def main(args: Array[String]): Unit = {

    //defaults if unspecified
    var k = 20
    var m:Int = 4
    var x = 3 //DEPRECATED, was 2nd-phase compression
    var b = 2000
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
    x = args(2).toInt //DEPRECATED, was 2nd-phase compression
    b = args(3).toInt
    useHT =  if(args(4).toInt == 1) true else false
    sequenceType = args(5).toInt
    inputDatasetPath = args(6)
    outputDatasetPath = args(7)
    prefix = args(8)
    write = if(args(9).toInt == 1) true else false
    useKryo = if(args(10).toInt == 1) true else false
    useCustomPartitioner = if(args(11).toInt == 1) true else false
    if(useCustomPartitioner)
      numPartitionTasks = args(12).toInt


    val tc = TestConfiguration(inputDatasetPath,outputDatasetPath,k,m,x,max_b=b,prefix=prefix,useHT=useHT,sequenceType=sequenceType,write=write,useCustomPartitioner=useCustomPartitioner,numPartitionTasks = numPartitionTasks,useKryoSerializer=useKryo)//hdfs://mycluster/tests/input/S2_8GB.fasta

    run(tc)


  }

  def run(configuration: TestConfiguration) {

    var conf = new SparkConf()
      .setAppName("SKC Test: k" + configuration.k + " m:" + configuration.m)
      .setMaster("local[4]")

    if(configuration.useKryoSerializer)
      conf = conf
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired", "true")
        .registerKryoClasses(Array(classOf[Kmer],classOf[Array[Object]],classOf[ArrayBuffer[Kmer]],classOf[Array[Int]],Class.forName("scala.reflect.ClassTag$$anon$1"),classOf[java.lang.Class[Any]]))


    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    SparkBinKmerCounter.executeJob(spark, configuration)

  }


}