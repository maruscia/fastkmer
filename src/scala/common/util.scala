package common

/**
  * Created by maru on 2/21/17.
  */
package object util {




  case class Signature(value: String, pos: Int)

  def repr(s: String, useCanonical:Boolean): String = {
    if(useCanonical) {
      val rev_comp = reverse_complement(s)
      if (rev_comp < s)
        rev_comp
    }
    s
  }

  def canonical_version(s: String): String = {
    val rev_comp = reverse_complement(s)
    if (s < rev_comp)
      s
    else rev_comp
  }


  def reverse_complement(s: String): String = s.reverse.map { case 'A' => 'T'  case 'C' => 'G'  case 'G' => 'C' case 'T' => 'A'  case c => c}

  ////but only such that do not start with AAA, neither start with ACA, neither contain AA anywhere except at their beginning.
  def is_valid(s: String): Boolean = !s.startsWith("AAA") && !s.startsWith("ACA") && s.indexOf("AA",1) < 0
}
