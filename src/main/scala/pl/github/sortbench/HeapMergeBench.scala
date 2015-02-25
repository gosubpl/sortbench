package pl.github.sortbench

import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

import scala.collection.mutable.{ListBuffer, PriorityQueue}

@State(Scope.Thread)
class HeapMergeBench {

  // a bit mutable heapsort using the StdLib mutable.PriorityQueue
  def sortHeapPQ(xs: Array[Int]): Array[Int] = {
    val ord = implicitly[Ordering[Int]].reverse
    val lst = ListBuffer[Int]()
    val pq: PriorityQueue[Int] = new PriorityQueue[Int]()(ord) ++ xs
    while (pq.size > 0) {
      val elem = pq.dequeue()
      lst += elem
    }
    lst.toArray
  }

  // impure HeapSort using the Scalaz.Heap for comparison
  def sortHeapLeftistImpure(xs: Array[Int]): Array[Int] = {
    import scalaz._
    import Scalaz._
    val lst = ListBuffer[Int]()
    var pq: Heap[Int] = Heap.fromData(xs.toList)
    while (pq.size > 0) {
      val elem = pq.minimum
      lst += elem
      pq = pq.deleteMin
    }
    lst.toArray
  }

  // pure functional HeapSort using the Scalaz.Heap
  // unfortunately Foldable implementation is internal to the package
  def sortHeapLeftist(xs: Array[Int]): Array[Int] = {
    import scalaz._
    import Scalaz._
    def poorMansHeapFold(h: Heap[Int]): List[Int] = {
      def heapFoldLeftAccum(accum: List[Int], h: Heap[Int]): List[Int] = {
        if (h.size == 0) {
          accum.reverse
        } else {
          val (head, tail) = h.uncons.get
          heapFoldLeftAccum(head :: accum, tail)
        }
      }
      heapFoldLeftAccum(Nil, h)
    }
    poorMansHeapFold(Heap.fromData(xs.toList)).toArray
  }

  // the same as above - Heap.sort uses the internal Foldable instance
  def sortHeapLeftistFast(xs: Array[Int]): Array[Int] = {
    import scalaz._
    import Scalaz._
    Heap.sort(xs.toList).toArray
  }

  // a nice Scala mergesort
  // Programming in Scala example improved by Daniel Sobral
  // http://stackoverflow.com/questions/2201472/merge-sort-from-programming-scala-causes-stack-overflow
  def mergeSort(xs: Array[Int]) : Array[Int] = msort[Int](_ < _)(xs.toList).toArray

  def msort[T](less: (T, T) => Boolean)
              (xs: List[T]): List[T] = {
    def merge(xs: List[T], ys: List[T], acc: List[T]): List[T] =
      (xs, ys) match {
        case (Nil, _) => ys.reverse ::: acc
        case (_, Nil) => xs.reverse ::: acc
        case (x :: xs1, y :: ys1) =>
          if (less(x, y)) merge(xs1, ys, x :: acc)
          else merge(xs, ys1, y :: acc)
      }
    val n = xs.length / 2
    if (n == 0) xs
    else {
      val (ys, zs) = xs splitAt n
      merge(msort(less)(ys), msort(less)(zs), Nil).reverse
    }
  }

  // array copy utility
  def copy[A: Manifest](array: Array[A]): Array[A] = {
    val newArray = new Array[A](array.size)
    Array.copy(array, 0, newArray, 0, array.size)
    newArray
  }

  // create array for sorting
  //val arr = Array.fill(2000000) { scala.util.Random.nextInt(5000000 - 1) }
  val arr = Array.fill(20000) {
    scala.util.Random.nextInt(5000000 - 1)
  }

  // results for 20000
  //  [info] Benchmark                                      Mode  Cnt    Score    Error  Units
  //  [info] HeapMergeBench.heapSortLeftistFastBench        avgt    5  149,657 ? 22,295  ms/op
  //  [info] HeapMergeBench.heapSortLeftistHeapBench        avgt    5  135,498 ?  2,278  ms/op
  //  [info] HeapMergeBench.heapSortLeftistHeapImpureBench  avgt    5  134,852 ?  2,171  ms/op
  //  [info] HeapMergeBench.heapSortPQBench                 avgt    5    4,869 ?  0,092  ms/op
  //  [info] HeapMergeBench.mergeSortBench                  avgt    5    9,494 ?  0,143  ms/op

  // results for 500000
  //  [info] Benchmark                                  Mode  Cnt     Score      Error  Units
  //  [info] QuickBench.mappingArraysWithBindBench      avgt    5    47,102 ?    8,551  ms/op
  //  [info] QuickBench.mappingArraysWithFlatMapBench   avgt    5    48,076 ?    7,175  ms/op
  //  [info] QuickBench.quicksortArrayBench             avgt    5    54,339 ?    0,778  ms/op
  //  [info] QuickBench.quicksortArrayBufferBench       avgt    5   112,387 ?    5,576  ms/op
  //  [info] QuickBench.quicksortFunctionalBench        avgt    5  1058,847 ?   11,982  ms/op
  //  [info] QuickBench.quicksortMonadicSTBench         avgt    5  5535,203 ? 2899,628  ms/op
  //  [info] QuickBench.quicksortStdLibBench            avgt    5    56,663 ?    3,047  ms/op
  //  [info] QuickBench.quicksortVectorBench            avgt    5   816,498 ?   99,439  ms/op
  //  [info] QuickBench.quicksortVectorFunctionalBench  avgt    5   865,723 ?   48,723  ms/op

  // results for 1000000
  //  [info] Benchmark                                  Mode  Cnt      Score      Error  Units
  //  [info] QuickBench.mappingArraysWithBindBench      avgt    5    129,738 ?   46,444  ms/op
  //  [info] QuickBench.mappingArraysWithFlatMapBench   avgt    5    140,983 ?   32,758  ms/op
  //  [info] QuickBench.quicksortArrayBench             avgt    5    112,694 ?    5,327  ms/op
  //  [info] QuickBench.quicksortArrayBufferBench       avgt    5    252,854 ?   60,956  ms/op
  //  [info] QuickBench.quicksortFunctionalBench        avgt    5   2192,502 ?   49,585  ms/op
  //  [info] QuickBench.quicksortMonadicSTBench         avgt    5  13423,726 ? 7254,023  ms/op
  //  [info] QuickBench.quicksortStdLibBench            avgt    5    116,997 ?    1,773  ms/op
  //  [info] QuickBench.quicksortVectorBench            avgt    5   1790,146 ?   70,729  ms/op
  //  [info] QuickBench.quicksortVectorFunctionalBench  avgt    5   1981,340 ?  129,203  ms/op

  // results for 2000000
  //  [info] Benchmark                                  Mode  Cnt      Score       Error  Units
  //  [info] QuickBench.mappingArraysWithBindBench      avgt    5    979,006 ?  2907,177  ms/op
  //  [info] QuickBench.mappingArraysWithFlatMapBench   avgt    5    832,711 ?  2388,770  ms/op
  //  [info] QuickBench.quicksortArrayBench             avgt    5    231,814 ?     2,516  ms/op
  //  [info] QuickBench.quicksortArrayBufferBench       avgt    5    560,954 ?    59,608  ms/op
  //  [info] QuickBench.quicksortFunctionalBench        avgt    5   4726,001 ?    74,034  ms/op
  //  [info] QuickBench.quicksortMonadicSTBench         avgt    5  35290,807 ? 10977,761  ms/op
  //  [info] QuickBench.quicksortStdLibBench            avgt    5    243,535 ?     7,372  ms/op
  //  [info] QuickBench.quicksortVectorBench            avgt    5   4684,715 ?   721,453  ms/op
  //  [info] QuickBench.quicksortVectorFunctionalBench  avgt    5   4375,908 ?   814,925  ms/op

  // copies of original array for sorting
  val arr1 = copy(arr)
  val arr2 = copy(arr)
  val arr3 = copy(arr)
  val arr4 = copy(arr)
  val arr5 = copy(arr)
  val arr6 = copy(arr)
  val arr7 = copy(arr)
  val arr8 = copy(arr)
  val arr9 = copy(arr)

  // to run the benchmarks
  // run -i 3 -wi 3 -f1 -t1 -bm avgt -tu us .*HeapMergeBench.*
  // run -i 3 -wi 3 -f1 -t1 -bm avgt -tu ms -jvmArgsAppend "-Xss512m" .*HeapMergeBench.*
  // run -i 5 -wi 5 -f1 -t1 -bm avgt -tu ms -jvmArgsAppend "-Xss1024m" .*HeapMergeBench.*

  @Benchmark
  def heapSortPQBench(): Unit = {
    val xz = sortHeapPQ(arr1)
    val ret = xz(0)
  }

  @Benchmark
  def heapSortLeftistHeapBench(): Unit = {
    val xz = sortHeapLeftist(arr2)
    val ret = xz(0)
  }

  @Benchmark
  def heapSortLeftistHeapImpureBench(): Unit = {
    val xz = sortHeapLeftistImpure(arr3)
    val ret = xz(0)
  }

  @Benchmark
  def heapSortLeftistFastBench(): Unit = {
    val xz = sortHeapLeftistFast(arr4)
    val ret = xz(0)
  }

  @Benchmark
  def mergeSortBench(): Unit = {
    val xz = mergeSort(arr5)
    val ret = xz(0)
  }
}