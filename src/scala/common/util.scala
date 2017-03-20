package common
import scala.reflect.ClassTag

/**
  * Created by maru on 2/21/17.
  */
package object util {

  val DEFAULT_SIGNATURE = "ZZZZ"

  //special signature to handle cases like

  case class Signature(value: String, pos: Int)


  def repr(s: String, canonical: Boolean): String = {
    var r = s

    if (canonical) {
      val rev_comp = reverse_complement(s)
      if (rev_comp < s) r = rev_comp
    }

    if (is_valid(r)) r else DEFAULT_SIGNATURE

  }


  def reverse_complement(s: String): String = s.reverse.map { case 'A' => 'T' case 'C' => 'G' case 'G' => 'C' case 'T' => 'A' case c => c }

  ////but only such that do not start with AAA, neither start with ACA, neither contain AA anywhere except at their beginning.
  def is_valid(s: String): Boolean = !s.startsWith("AAA") && !s.startsWith("ACA") && s.indexOf("AA", 1) < 0

  def nucleotideToInt(n: Char): Int = n match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case _ => 4
  }


  def getOrientation(kmer: String): Int = {
    /** Checks lexicographic orientation of a kmer string.
      *
      * @param kmer the kmer string*
      * @return 0 for original orientation, 1 for reverse complement
      */
    def _orientation(i: Int, j: Int): Int = (kmer.charAt(i), kmer.charAt(j)) match {
      case (start, end) if start < end => 0
      case (start, end) if start > end || i >= j => 1
      case _ => _orientation(i + 1, j - 1)
    }

    _orientation(0, kmer.length() - 1)
  }


}

