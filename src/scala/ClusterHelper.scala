import org.apache.spark.sql.SparkSession


object Main {
  object Configuration{
    val K = 20    //args(1).toInt
    val N = 100   //args(2).toInt
    val M = 4     //args(3).toInt
    val BOTHSTRANDS = true  //args(4).toBoolean

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir","wasbs:///spark-warehouse")
      .appName("SparkKmerCount")
      .getOrCreate()

    SparkKmerCounter.executeJob(spark, "wasbs://sparkcontainer@grupporisorse13742.blob.core.windows.net/datasets/dataset_100MB.fasta","wasbs:///output/dataset_100MB.txt")

  }
}
