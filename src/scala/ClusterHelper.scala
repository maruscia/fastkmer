import org.apache.spark.sql.SparkSession


object Main {
  object Configuration{
    val K = 15   //kmers length
    val M = 4     //signature length
    val BOTHSTRANDS = true  //if true, canonical version will be used

    //debug
    val X = 1   //as in (k,X)-mers, length of splitted super-kmers
    val N = 200   //number of top results
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir","wasbs:///spark-warehouse")
      .appName("SparkKmerCount")
      .getOrCreate()

    SparkKmerCounter.executeJob(spark, "wasbs://sparkcontainer@grupporisorse13742.blob.core.windows.net/datasets/dataset_1.5GB.fasta","wasbs:///output/KMC2_dataset_1.5GB_2.txt")
    //SparkBasicKmerCounter.executeJob(spark, "wasbs://sparkcontainer@grupporisorse13742.blob.core.windows.net/datasets/dataset_1.5GB.fasta","wasbs:///output/Basic_dataset_1.5GB.txt")

  }
}
