package common
import scala.reflect.ClassTag
import common.util._
/**
  * Created by Mara Sorella on 3/20/17.
  */
package object sorting{

  /*TODO: provide better sorting implementation (MSD, possibly in place), taking RS for granted now*/

  object CountSort {
    def apply[T : ClassTag](unsorted: Iterable[T],radix: Short,key: T => Short): Iterable[T] = {
      if(unsorted.isEmpty)
        return unsorted
      val sorted: Array[T] = Array.ofDim(unsorted.size)
      unsafeSortUsingAuxiliary(unsorted, radix, key, sorted)
      sorted.toIterable
    }

    def unsafeSortUsingAuxiliary[T : ClassTag](
                                                unsorted: Iterable[T],
                                                radix: Short,
                                                key: T => Short,
                                                auxiliary: Array[T]
                                              ): Unit = {

      require(radix > 0)

      // Count occurrences of each key
      val counts = Array.fill(radix + 1)(0)
      for (item <- unsorted) {

        val keyForItem = key(item)
        require(keyForItem >= 0)
        counts(keyForItem + 1) += 1
      }

      // Accumulate counts to create offsets for all keys
      for (i <- 0 until radix) {
        counts(i + 1) += counts(i)
      }

      // Put items in the sorted order, in a new array
      for (item <- unsorted) {
        val keyForItem = key(item)
        val indexWhenSorted = counts(keyForItem)
        auxiliary(indexWhenSorted) = item
        counts(keyForItem) += 1
      }
    }



    def apply(unsorted: Iterable[Short], radix: Short): Iterable[Short] = {
      apply(unsorted, radix, (identity[Short] _))
    }

  }

  object RadixLSDSort {
    /*
     * Sorts `unsorted` on the `stringLength` leading characters
     */
    def apply(
               unsorted: Iterable[String],
               stringLength: Int,
               radix: Short = 4
             ): Array[String] = {
      val toSort = unsorted.toArray.clone
      unsafeSortInplace(toSort, stringLength, radix)
      toSort
    }

    def unsafeSortInplace(
                           toSort: Array[String],
                           stringLength: Int,
                           radix: Short = 4
                         ): Unit = {
      val n = toSort.size
      val auxiliary = Array.ofDim[String](n)
      for (d <- (stringLength - 1) to 0 by -1) {
        CountSort.unsafeSortUsingAuxiliary(
          toSort,
          radix,
          (s: String) => nucleotideToShort(s.charAt(d)),
          auxiliary
        )
        auxiliary.copyToArray(toSort)
      }
    }
  }




}
