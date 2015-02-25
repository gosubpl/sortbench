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
  val arr = Array.fill(1000000) {
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
  //  [info] Benchmark                                      Mode  Cnt      Score       Error  Units
  //  [info] HeapMergeBench.heapSortLeftistFastBench        avgt    5  33472,645 ? 13342,665  ms/op
  //  [info] HeapMergeBench.heapSortLeftistHeapBench        avgt    5   6778,557 ?  2001,965  ms/op
  //  [info] HeapMergeBench.heapSortLeftistHeapImpureBench  avgt    5   6337,421 ?  3852,705  ms/op
  //  [info] HeapMergeBench.heapSortPQBench                 avgt    5    330,861 ?    55,857  ms/op
  //  [info] HeapMergeBench.mergeSortBench                  avgt    5    410,712 ?    20,858  ms/op

  // results for 1000000
  //  [info] Benchmark                                      Mode  Cnt      Score      Error  Units
  //  [info] HeapMergeBench.heapSortLeftistFastBench        java.lang.OutOfMemoryError: GC overhead limit exceeded
  //  [info] HeapMergeBench.heapSortLeftistHeapBench        avgt    5  15368,208 ? 4621,253  ms/op
  //  [info] HeapMergeBench.heapSortLeftistHeapImpureBench  avgt    5  13297,206 ?  913,167  ms/op
  //  [info] HeapMergeBench.heapSortPQBench                 avgt    5    834,162 ?  188,393  ms/op
  //  [info] HeapMergeBench.mergeSortBench                  avgt    5   1035,488 ?  264,340  ms/op

  // results for 2000000
  //  did not run

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