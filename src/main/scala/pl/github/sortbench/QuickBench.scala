package pl.github.sortbench

import org.openjdk.jmh.annotations.{Benchmark, Scope, State}

import scala.collection.mutable.ArrayBuffer
import scalaz.Scalaz._
import scalaz._
import scalaz.effect.ST._
import scalaz.effect._

@State(Scope.Thread)
class QuickBench {

  // The best of the best - well known QuickSorts
  // This particular implementations adapted from
  // http://www.scala-lang.org/docu/files/ScalaByExample.pdf
  // the Vector functional qsort seems even more functional!
  def sortQuickFunctional(xs: Array[Int]): Array[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Array.concat(sortQuickFunctional(xs filter (pivot >)), xs filter (pivot ==), sortQuickFunctional(xs filter (pivot <)))
    }
  }

  def sortQuickVectorFunctional(xs: Vector[Int]): Vector[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Vector.concat(sortQuickVectorFunctional(xs filter (pivot >)), xs filter (pivot ==), sortQuickVectorFunctional(xs filter (pivot <)))
    }
  }


  def sortQuickTraditional(xs: Array[Int]): Array[Int] = {
    def swap(i: Int, j: Int) {
      val t = xs(i)
      xs(i) = xs(j)
      xs(j) = t
    }

    def sort1(l: Int, r: Int) {
      val pivot = xs((l + r) / 2)
      var i = l
      var j = r
      while (i <= j) {
        while (xs(i) < pivot) i += 1
        while (xs(j) > pivot) j -= 1
        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }
      if (l < j) sort1(l, j)
      if (j < r) sort1(i, r)
    }
    sort1(0, xs.length - 1)
    xs
  }

  // Imperative QuickSort using Scalaz ST Monad
  // https://github.com/fpinscala/fpinscala/blob/master/answers/src/main/scala/fpinscala/localeffects/LocalEffects.scala
  def sortQuickMonadicST(xi: Array[Int]): Array[Int] = {
    def invert(x: (Int, Int)): (Int, Int) = (x._2, x._1)
    def identify(x: Int, y: Int): Int = y
    val arrLen = xi.length
    val xiz = xi.zipWithIndex
    val xizi = (xiz map invert).toList

    type ForallST[A] = Forall[({type λ[S] = ST[S, A]})#λ]
    def noop[S] = ST[S, Unit](())

    def swap[S](a: STArray[S, Int], i: Int, j: Int): ST[S, Unit] = for {
      x <- a.read(i)
      y <- a.read(j)
      _ <- a.write(i, y)
      _ <- a.write(j, x)
    } yield ()

    def partition[S](a: STArray[S, Int], l: Int, r: Int, pivot: Int): ST[S, Int] = for {
      vp <- a.read(pivot)
      _ <- swap(a, pivot, r)
      j <- newVar(l)
      _ <- (l until r).foldLeft(noop[S])((s, i) => for {
        _ <- s
        vi <- a.read(i)
        _ <- if (vi < vp) (for {
          vj <- j.read
          _ <- swap(a, i, vj)
          _ <- j.write(vj + 1)
        } yield ())
        else noop[S]
      } yield ())
      x <- j.read
      _ <- swap(a, x, r)
    } yield x

    def qs[S](a: STArray[S, Int], l: Int, r: Int): ST[S, Unit] = if (l < r) for {
      pi <- partition(a, l, r, l + (r - l) / 2)
      _ <- qs(a, l, pi - 1)
      _ <- qs(a, pi + 1, r)
    } yield ()
    else noop[S]

    def e1[S] = for {
      arr <- newArr[S, Int](arrLen, 0)
      _ <- arr.fill(identify, xizi)
      _ <- qs(arr, 0, arr.size - 1)
      sorted <- arr.freeze
    } yield sorted

    runST(new ForallST[ImmutableArray[Int]] {
      def apply[S] = e1[S]
    }).toArray
  }

  // variations of the ScalaByExample imperative Quicksort with ArrayBuffer and Vector
  def sortQuickArrBufTraditional(xi: Array[Int]): Array[Int] = {
    val xs = ArrayBuffer(xi: _*)
    def swap(i: Int, j: Int) {
      val t = xs(i)
      xs(i) = xs(j)
      xs(j) = t
    }

    def sort1(l: Int, r: Int) {
      val pivot = xs((l + r) / 2)
      var i = l
      var j = r
      while (i <= j) {
        while (xs(i) < pivot) i += 1
        while (xs(j) > pivot) j -= 1
        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }
      if (l < j) sort1(l, j)
      if (j < r) sort1(i, r)
    }
    sort1(0, xs.length - 1)
    xs.toArray
  }

  def sortQuickVectorTraditional(xs: Array[Int]): Array[Int] = {
    var xv: Vector[Int] = xs.toVector
    def swap(i: Int, j: Int) {
      val t = xv(i)
      xv = xv.updated(i, xv(j))
      xv = xv.updated(j, t)
    }

    def sort1(l: Int, r: Int) {
      val pivot = xv((l + r) / 2)
      var i = l
      var j = r
      while (i <= j) {
        while (xv(i) < pivot) i += 1
        while (xv(j) > pivot) j -= 1
        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }
      if (l < j) sort1(l, j)
      if (j < r) sort1(i, r)
    }
    sort1(0, xv.length - 1)
    xv.toArray
  }

  // array copy utility
  def copy[A: Manifest](array: Array[A]): Array[A] = {
    val newArray = new Array[A](array.size)
    Array.copy(array, 0, newArray, 0, array.size)
    newArray
  }

  // create array for sorting
  //val arr = Array.fill(2000000) { scala.util.Random.nextInt(5000000 - 1) }
  val arr = Array.fill(2000000) {
    scala.util.Random.nextInt(5000000 - 1)
  }

  // results for 20000
  //  [info] Benchmark                                  Mode  Cnt    Score   Error  Units
  //  [info] QuickBench.mappingArraysWithBindBench      avgt    5    1,420 ? 0,042  ms/op
  //  [info] QuickBench.mappingArraysWithFlatMapBench   avgt    5    1,382 ? 0,016  ms/op
  //  [info] QuickBench.quicksortArrayBench             avgt    5    1,708 ? 0,029  ms/op
  //  [info] QuickBench.quicksortArrayBufferBench       avgt    5    3,196 ? 0,032  ms/op
  //  [info] QuickBench.quicksortFunctionalBench        avgt    5   33,467 ? 0,365  ms/op
  //  [info] QuickBench.quicksortMonadicSTBench         avgt    5  138,314 ? 5,174  ms/op
  //  [info] QuickBench.quicksortStdLibBench            avgt    5    1,790 ? 0,041  ms/op
  //  [info] QuickBench.quicksortVectorBench            avgt    5   17,123 ? 0,428  ms/op
  //  [info] QuickBench.quicksortVectorFunctionalBench  avgt    5   22,879 ? 0,506  ms/op

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
  // run -i 3 -wi 3 -f1 -t1 -bm avgt -tu us .*QuickBench.*
  // run -i 3 -wi 3 -f1 -t1 -bm avgt -tu ms -jvmArgsAppend "-Xss512m" .*QuickBench.*
  // run -i 5 -wi 5 -f1 -t1 -bm avgt -tu ms -jvmArgsAppend "-Xss1024m" .*QuickBench.*

  @Benchmark
  def quicksortArrayBench(): Unit = {
    val arr1a = copy(arr1)
    val xz = sortQuickTraditional(arr1a)
    val ret = xz(0)
  }

  @Benchmark
  def quicksortVectorBench(): Unit = {
    val arr2a = copy(arr2)
    val xz = sortQuickVectorTraditional(arr2a)
    val ret = xz(0)
  }

  @Benchmark
  def quicksortArrayBufferBench(): Unit = {
    val arr3a = copy(arr3)
    val xz = sortQuickArrBufTraditional(arr3a)
    val ret = xz(0)
  }

  @Benchmark
  def quicksortFunctionalBench(): Unit = {
    val xz = sortQuickFunctional(arr4)
    val ret = xz(0)

  }

  @Benchmark
  def quicksortVectorFunctionalBench(): Unit = {
    val xz = sortQuickVectorFunctional(arr5.toVector)
    val ret = xz(0)
  }

  @Benchmark
  def quicksortMonadicSTBench(): Unit = {
    val xz = sortQuickMonadicST(arr6)
    val ret = xz(0)
  }

  @Benchmark
  def quicksortStdLibBench(): Unit = {
    val arr7a = copy(arr7)
    scala.util.Sorting.quickSort(arr7a)
    val ret = arr7(0)
  }

  @Benchmark
  def mappingArraysWithFlatMapBench(): Unit = {
    val xz = arr8.toList flatMap { x => List(-x)}
    val ret = xz(0)
  }

  @Benchmark
  def mappingArraysWithBindBench(): Unit = {
    val xz = (arr9.toList) >>= { x => List(-x)}
    val ret = xz(0)
  }
}