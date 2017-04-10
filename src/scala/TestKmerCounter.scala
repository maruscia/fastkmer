import org.apache.spark.sql.SparkSession
import common.util.TestConfiguration
import org.apache.spark.{SparkConf, SparkContext}


object TestKmerCounter {


  def main(args: Array[String]): Unit = {

    //parse command line arguments
    val datasetFolder = "hdfs://mycluster/tests/input/"

    //defaults if unspecified
    var k = 20
    var m:Int = 4
    var x = 3
    var dataset =  datasetFolder + "S2_1GB.fasta"
    var prefix = "TIMING"

    k = args(0).toInt
    m = args(1).toInt
    x = args(2).toInt
    dataset = datasetFolder + args(3)
    /*
    args.sliding(2, 1).toList.collect {
      case Array("-k", argK: String) => k = argK.toInt
      case Array("-m", argM: String) => m = argM.toInt
      case Array("-x", argX: String) => x = argX.toInt
      case Array("-d", argDataset: String) => dataset = datasetFolder + argDataset
      case Array("-p", argPrefix: String) => prefix = argPrefix
    }*/

    val tc = TestConfiguration(dataset,k,m,x,prefix=prefix)//hdfs://mycluster/tests/input/S2_8GB.fasta


    run(tc)

    //debug(tc)

    //"wasbs://sparkcontainer@grupporisorse13742.blob.core.windows.net/datasets/dataset_1.5GB.fasta","wasbs:///output/KMC2_dataset_1.5GB_last2.txt")
    /*val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", "wasbs:///spark-warehouse")
      .appName("Basic")
      .getOrCreate()
    SparkBasicKmerCounter.executeJob(spark, tc)
*/
  }

  def run(configuration: TestConfiguration) {
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", "wasbs:///spark-warehouse")
      .appName("SKC Test: k" + configuration.k + " m:" + configuration.m)
      .getOrCreate()

    SparkKmerCounter.executeJob(spark, configuration)


  }


  def debug(configuration: TestConfiguration): Unit ={

    val conf = new SparkConf().setAppName("SparkSample")
      .setMaster("yarn-client")
      .set("spark.driver.extraJavaOptions","-Dhdp.version=2.5.4.0-121")
      .set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=2.5.4.0-121")
      .set("spark.sql.warehouse.dir", "wasbs:///spark-warehouse")
      .set("spark.yarn.jars", "wasbs:///datasets@grupporisorse13742.blob.core.windows.net/jars/spark-assembly-2.0.0-hadoop2.7.0-SNAPSHOT.jar")
      .setJars(Seq("""/Users/maru/Dropbox/Dottorato/2017/scala/KmerCounting/out/artifacts/KmerCounting_DefaultArtifact/default_artifact.jar"""))
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    //SparkKmerCounter.executeJob(sc,configuration)
  }
}