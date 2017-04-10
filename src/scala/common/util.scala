package common


import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.util.SizeEstimator

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.util.hashing.{MurmurHash3 => MH3}


/**
  * Created by Mara Sorella on 2/21/17.
  */
package object util {

  /*
  * TESTS
  * */
  case class TestConfiguration(dataset: String,k: Int, m: Int, x: Int, max_b: Int = 512,canonical:Boolean=true,prefix:String = ""){
    val b:Int = Math.min(Math.pow(4,m),max_b).toInt
    val outputDir:String = "hdfs://mycluster/tests/output/"+prefix+"k" + k + "_m" + m + "_x" + x + "_b" + b

    override def toString: String = "Kmer counting on Spark. \nTest parameters:\nDataset: "+dataset + "\nk: "+ k + "\nm: " + m + "\nx: " + x + "\nb: " + b
  }


  /* DEBUG FUNCTIONS */
  def getDateDiff(date1:Date, date2: Date, timeUnit:TimeUnit) = {
    val diffInMillies = date2.getTime() - date1.getTime()
    timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS)
  }

  def estimateSize(obj: AnyRef): String  ={
    SizeEstimator.estimate(obj) / (1024 * 1024) + "MB"
  }

  /* END DEBUG */



  case class Signature(value: String, pos: Int)

  class RIndex(arr: Array[String],startPos: Int, endPos: Int, shift: Int, kmer_length: Int) extends Ordered[RIndex]{
    /*
    Class that represent a k-mer scanner for the array R
    *
    */

    private var currentPos: Int = startPos
    var pointedKmer: String = _

    _readKmer()


    def advance(): Unit = {
      currentPos +=1
      if(!exhausted)
        _readKmer()
    }

    def _readKmer(): Unit ={
      pointedKmer = arr(currentPos).substring(shift, shift + kmer_length)
    }

    def exhausted: Boolean = currentPos == endPos

    //Indexes are compared lexicographically on the pointed kmer string.
    def compare(that: RIndex):Int = this.pointedKmer compare that.pointedKmer

    override def toString: String = "[A: " + arr.hashCode() + " start: " + startPos + " end: "+endPos +" shift: "+shift+"] : "+ pointedKmer + "("+currentPos+")"

  }

  object PointedMinOrder extends Ordering[RIndex] {
    /*
    * This object relies on the compare method of RIndex to define a (min) order.
    * It is intended to be used as ordering for the priority of kmer pointers (RIndexes) contained in the heap.
    * The default ordering relies on compare to form a _max_ heap.
    * Therefore the order of items compared must be reversed (y compare x) to enforce a min heap.
    * See the overriden compare method of RIndex for further information.
    * */

    def compare(x:RIndex, y:RIndex):Int = y compare x
  }

  def repr(s: String, canonical: Boolean): String = {
    var r:String = s

    if(canonical) {
      val rev_comp = reverse_complement(s)
      if (rev_comp < s)
        r = rev_comp
    }
    r
  }

  def mMerRepr(s: String, canonical: Boolean) = {

    val r = repr(s,canonical)
    if(is_valid(r))
      r
    else
      DEFAULT_SIGNATURE_STRING
  }

  def nucleotide_complement(c: Char): Char = c match { case 'A' => 'T' case 'C' => 'G' case 'G' => 'C' case 'T' => 'A' case x => x}

  def notANucleotide(c: Char): Boolean = !(c == 'A' || c == 'C' || c == 'G' || c == 'T')


  def containsOnlyValidNucleotides(s: String): Boolean = {
    for(c <- s){
      if(notANucleotide(c)) return false
    }
    true
  }

  //def reverse_complement(s: String): String = s.reverse.map { c => nucleotide_complement(c) }

  def reverse_complement(s: String): String = {
    var sb = ""//new StringBuilder()
    for (i <- s.length-1 to 0 by -1){
      sb += (nucleotide_complement(s.charAt(i)))
    }
    sb//.toString
  }


  ////"but only such that do not start with AAA, neither start with ACA, neither contain AA anywhere except at their beginning."
  def is_valid(s: String): Boolean =  containsOnlyValidNucleotides(s) && !s.startsWith("AAA") && !s.startsWith("ACA") && s.indexOf("AA", 1) < 0

  def nucleotideToShort(n: Char): Short = n match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case x => throw new Exception("Not a nucleotide: " + x)
  }

  /*def minimumSignature(s: String, m: Int, s_starting_pos:Int, canonical: Boolean): Signature = {
    val tuple = s.sliding(m,1).zipWithIndex.map{ case (str,i) => (mMerRepr(str,canonical),i)}.min
    Signature(tuple._1,tuple._2 + s_starting_pos)
  }*/

  def minimumSignature (s: String, m: Int, s_starting_pos:Int, canonical: Boolean): Signature = {
    var min_s = DEFAULT_SIGNATURE_STRING
    var min_pos = 0
    var cur = ""

    for (i <- 0 to s.length - m) {
      cur = mMerRepr(s.substring(i,i+m),canonical)
      if(cur < min_s){
        min_s = cur
        min_pos = i
      }
    }
    Signature(min_s,s_starting_pos + min_pos)
  }




  def hash_to_bucket(s: String, B: Int): Int = {
    (MH3.stringHash(s, MH3.stringSeed) & 0xffffffffL % B).toInt //((s.hashCode & 0xFFFFFFFFL) % B).toInt
  }

  def getOrientation(kmer: String): Int = {
    /** Checks lexicographic orientation of a kmer string.
      *
      *  @param kmer the kmer string
      *  @return 0 for original orientation, 1 for reverse complement
      */
    def _orientation(i: Int, j: Int): Int = (kmer.charAt(i),kmer.charAt(j)) match {
      case (start,end) if start < nucleotide_complement(end)  => 0
      case (start,end) if start > nucleotide_complement(end) || (i >= j) => 1
      case _ => _orientation(i+1,j-1)
    }
    _orientation(0,kmer.length()-1)
  }

  def getCanonicalSubstring(s: String, start: Int, end: Int, orientation: Int): String =  orientation match {
    case 1 => reverse_complement(s.substring(start,end))
    case 0 => s.substring(start,end)
  }

  //TODO: Think if it makes sense to curry k instead
  def priorityQueueWithIndexes(arr: Array[Array[String]],k: Int): mutable.PriorityQueue[RIndex] = {

    var pos = 0

    var starts = Array[Int]()
    var heap = PriorityQueue.empty[RIndex](PointedMinOrder)

    // for each array
    for (r_index <- arr.indices) {

      val a = arr(r_index)

      if (a.nonEmpty) {
        pos = 0
        starts = Array.fill[Int](r_index)(pos)

        //put one index for prefix (shift = 0) of each array
        heap.enqueue(new RIndex(a, pos, a.length, 0,k))
        if (a.length > 1) {

          for (j <- 1 until a.length) {
            for (i <- 0 until r_index) {

              if (a(j - 1).substring(0, i + 1) != a(j).substring(0, i + 1)) {
                val r = new RIndex(a, starts(i), j, i + 1,k)

                heap.enqueue(r)
                starts(i) = j
              }
            }
          }
        }
        for (i <- starts.indices) {
          val r = new RIndex(a, starts(i), a.length, i + 1,k)
          heap.enqueue(r)
        }
      }

    }
    heap
  }

  val DEFAULT_SIGNATURE_STRING = "ZZZZ"


  def firstAndLastOccurrenceOfInvalidNucleotide(c: Char,s:String): (Int,Int) = {
    //finds first and last occurrence of c in s, (-1,-1) if not present
    val r = s.foldLeft(-1,-1,0)({ case (f_l_a, ch) =>
      if (notANucleotide(ch)) {
        if (f_l_a._1 == -1)
          (f_l_a._3, f_l_a._3, f_l_a._3 + 1)
        else
          (f_l_a._1, f_l_a._3, f_l_a._3 + 1)
      }
      else (f_l_a._1, f_l_a._2, f_l_a._3 + 1)
    })
    (r._1,r._2)
  }
}

