package common


import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.reflect.ClassTag

/**
  * Created by Mara Sorella on 2/21/17.
  */
package object util {



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
      DEFAULT_SIGNATURE
  }


  def nucleotide_complement(c: Char): Char = c match { case 'A' => 'T' case 'C' => 'G' case 'G' => 'C' case 'T' => 'A' case x => x}


  def reverse_complement(s: String): String = s.reverse.map { c => nucleotide_complement(c) }


  ////"but only such that do not start with AAA, neither start with ACA, neither contain AA anywhere except at their beginning."
  def is_valid(s: String): Boolean = !s.startsWith("AAA") && !s.startsWith("ACA") && s.indexOf("AA", 1) < 0


  def nucleotideToShort(n: Char): Short = n match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case _ => 4
  }

  def minimumSignature(s: String, m: Int, s_starting_pos:Int, canonical: Boolean): Signature = {
    val tuple = s.sliding(m,1).zipWithIndex.map{ case (str,i) => (repr(str,canonical),i)}.min
    Signature(tuple._1,tuple._2 + s_starting_pos)
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

      a.foreach(println)
      println()
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

  val DEFAULT_SIGNATURE = "ZZZZ"
}

