package fastkmer.test

import fastkmer.SparkBinKmerCounter
import fastkmer.util.Kmer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import testutil._

import scala.collection.mutable.ArrayBuffer


object TestKmerCounter {


  def main(args: Array[String]): Unit = {

    //defaults if unspecified
    var k = 20
    var m:Int = 4
    var x = 3
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
    x = args(2).toInt
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


    val tc = TestConfiguration(inputDatasetPath,outputDatasetPath,k,m,x,max_b=b,prefix=prefix,useHT=useHT,sequenceType=sequenceType,write=write,useKryoSerializer=useKryo,useCustomPartitioner=useCustomPartitioner,numPartitionTasks=numPartitionTasks)//hdfs://mycluster/tests/input/S2_8GB.fasta

    run(tc)

  }

  def run(configuration: TestConfiguration) {


    var conf = new SparkConf()
      .setAppName("k" + configuration.k + " m" + configuration.m + " B" + configuration.b +" w" + {if(configuration.write)1 else 0} + " HT" + {if(configuration.useHT)1 else 0} + " P" + {if(configuration.useCustomPartitioner)1 else 0} +" PT" + configuration.numPartitionTasks)

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
    //SparkBinKmerCounter.executeFindBinSignaturesJob(spark, configuration)



  }

}