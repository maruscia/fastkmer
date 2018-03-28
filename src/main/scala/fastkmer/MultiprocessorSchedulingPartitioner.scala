package fastkmer

import org.apache.spark.Partitioner

import scala.util.Random


/**
  * Created by Mara Sorella on 6/7/17.
  */
class MultiprocessorSchedulingPartitioner(partitions : Int, binSizes:Array[(Int,Int)]) extends Partitioner{
  override def numPartitions: Int = partitions

  val partitionsMap:collection.mutable.HashMap[Int,Int] = MultiprocessorSchedulingPartitioner.solve(binSizes,partitions)

  override def getPartition(key: Any): Int = {
    if(partitionsMap.contains(key.asInstanceOf[Int]))
      partitionsMap.getOrElse(key.asInstanceOf[Int],-1)
    else{
      val dest = nonNegativeMod(key.hashCode, numPartitions)
      //println("Key not found("+key.asInstanceOf[Int]+"), resorting to: " + dest)
      dest
    }
    //partitionsMap.getOrElse(key.asInstanceOf[Int],nonNegativeMod(key.hashCode, numPartitions))
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}

object MultiprocessorSchedulingPartitioner{

  def solve(items:Array[(Int,Int)],partitions:Int):collection.mutable.HashMap[Int,Int] = {
    //println("Called Solve")
    //println("items: " + items.mkString(" "))
    //println("Total no of bins seen in sampling: "+items.length)

    val map = new collection.mutable.HashMap[Int, Int]()
    //create a set of job_bins with capacity equal to max(max_bin_size,sum_sizes/no_partitions)
    val bins: Array[Int] = Array.fill[Int](partitions)(0)//Math.max(items.map(_._2).max,Math.ceil(items.map(_._2).sum.toDouble/partitions)).toInt)
    //println("bins sizes: " + bins.deep.mkString(" "))
    val partitionIndexPermutation = new scala.util.Random(31337).shuffle[Int, IndexedSeq](0 until partitions)

    for (item <- items) {

      val bin = findBinLPT(item)
      //if (bin == -1) println("Bin not found for object: " + item)

      //else {
        scheduleOn(bin, item)

        map += (item._1 -> partitionIndexPermutation(bin))
        //println(item._1 +" -> "+ +bin + " -> " +item._2)
      //}
    }

    def scheduleOn(binNumber:Int,item:(Int,Int)): Unit = bins(binNumber) += item._2


    def findBinLPT(item:(Int,Int)): Int = {
      bins.zipWithIndex.minBy(_._1)._2
    }


    map
  }
}