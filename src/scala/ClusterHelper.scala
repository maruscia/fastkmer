import org.apache.spark.sql.SparkSession


object Main {
  object Configuration{
    //debug
    val X = 1   //as in (k,X)-mers, length of splitted super-kmers
    val N = 200   //number of top results
    val MAX_B:Int = 200 //max number of bins

    val K = 15   //kmers length
    val M:Int = 4     //signature length
    val B:Int = Math.min(Math.pow(4,M),MAX_B).toInt//number of bins
    val BOTHSTRANDS = true  //if true, canonical version will be used
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir","wasbs:///spark-warehouse")
      .appName("SparkKmerCount")
      .getOrCreate()

    SparkKmerCounter.executeJob(spark, "wasbs://sparkcontainer@grupporisorse13742.blob.core.windows.net/datasets/dataset_1.5GB.fasta","wasbs:///output/KMC2_dataset_1.5GB_giacomo_fesso.txt")
    //SparkBasicKmerCounter.executeJob(spark, "wasbs://sparkcontainer@grupporisorse13742.blob.core.windows.net/datasets/dataset_1.5GB.fasta","wasbs:///output/Basic_dataset_1.5GB.txt")

  }
}
