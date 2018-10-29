package com.amazon.deequ.anomalydetection.seasonal

import breeze.linalg.DenseVector
import breeze.math.Complex
import org.scalatest.{Matchers, WordSpec}

class FourierSeriesTest extends WordSpec with Matchers {

  "fft" should {
//    "breeze" in {
//      def aic(k: Int, rss: Double, n: Int): Double = {
//        2*k + n*math.log(rss/n)
//      }
//      def rss(y: DenseVector[Double], yHat: DenseVector[Double]): Double = {
//        val x = (y - yHat).map(math.pow(_, 2))
//        breeze.linalg.sum(x)
//      }
//      def ev(x: Int, yf: DenseVector[Complex]): Double = {
//        var total = Complex(0, 0)
//        val N = yf.length
//        for(b <- 0 until N) {
//          val h = Complex(math.cos(2*math.Pi*x*b/N), math.sin(2*math.Pi*x*b/N))
//          total += yf(b) * h
//        }
//        total.real
//      }
//
//      def modelSelection(y: DenseVector[Double]): DenseVector[Complex] = {
//        import breeze.signal._
//
//        // frequency bins
//        // 0(DC), Fs/n, 2*Fs/n, 3*Fs/n, ..., (Fs/2)*n/n, ..., 3*Fs/n, 2*Fs/n, Fs/n
//        val yf = fourierTr(y)
////        val freqs = fourierFreq(y.length, dt=3600*24)
//        val N = y.length
//        val results = collection.mutable.HashMap.empty[Int, Double]
//        for(k <- 1 until y.length) {
//          val yf_ = yf.copy
//
//          // remove model parameters
//          (k until y.length).foreach(yf_(_) = Complex(0, 0))
//
//          val fc = (0 until yf_.length).map(ev(_, yf_) / yf_.length)
//          val RSS = rss(y, DenseVector(fc.toArray))
//          val aic_ = aic(k, RSS, N)
//          results(k) = aic_
//        }
//        val (optimalK, aic_) = results.minBy(_._2)
//        (optimalK until y.length).foreach(yf(_) = Complex(0, 0))
//        yf
//      }
//
//      val y = DenseVector(Array[Double](1, 1, 1, 1, 1, 2, 2))
//
//      val result = modelSelection(y)
//      val fc = (0 until 2*y.length).map(ev(_, result))
//      val r = 1
//    }

    "actual strat" in {
      val s = FourierSeries(FourierSeries.DetectionMode.Auto, FourierSeries.MetricInterval.Daily)
      val result = s.detect(Vector[Double](1, 1, 1, 1, 1, 2, 2))
      val x = 1
    }

//    "work with math commons" in {
//      val f = new UnivariateFunction {
//        override def value(v: Double) = {
//          1
//        }
//      }
//
//      val fs = 1.0d
//      scala.math.cos(2*scala.math.Pi*fs)
//      val t = new FastFourierTransformer(DftNormalization.STANDARD)
//      // 0, 1/8, 2/8, 3/8, 4/8 Nyq, 3/8, 2/8, 1/8
//      val x = t.transform(Array[Double](1, 2, 1, 2), TransformType.FORWARD)
////      val xmod = Array[Complex]()
//      x(2).subtract(4)
//      println(x)
//      val xx = t.transform(x, TransformType.INVERSE)
////      val xx = t.transform(f, 0, 7, 3, TransformType.FORWARD)
////      val xxx = t.transform(f, 0, 7, 3, TransformType.INVERSE)
////      val x = t.transform(Array(1, 1, 1, 1), TransformType.FORWARD)
//      val a = 1
//    }


    //  "periodogram" should {
    //    "work for whole week data" in {
    //      import breeze.signal._
    //      val y = DenseVector(List.fill(5)(List[Double](1, 1, 1, 1, 1, 2, 2)).flatten.toArray)
    //      val yf = fourierTr(y)
    //      val yfn = yf.data.head/y.length
    //      val freqs = fourierFreq(windowLength = y.length, fs = y.length) // whole signal within one second
    //      // drop DC
    //      val abc = freqs.toArray.tail.zip(yf.map(_.abs).toArray.tail)//.maxBy(_._2)
    //      val (pitch, mag) = abc.maxBy(_._2)
    //      val periodLength = y.length/pitch
    //      if (periodLength == 7) {
    //        println("yay")
    //      }
    ////      val frqs = List(0, 1.0*y.length/y.length, 2.0*y.length/y.length)
    //      val x = 1
    //    }
    //
    //    "work for partial weekly data" in {
    //      def periodicityFor(series: Array[Double], relativePowerThreshold: Double = 0.35): Option[Int] = {
    //        import breeze.signal._
    //        val y = DenseVector(series)
    //        // window
    ////        val c = convolve(y_, DenseVector(1.0, 1.0, 1.0, 1.0, 1.0), overhang = PreserveLength)
    ////        val sigAndWindowed = y.toArray.zip(c.toArray)
    //
    //        val yf = fourierTr(y)
    //        // mean of series
    //        val yfn = yf.data.head/y.length
    //
    //        val n = y.length
    //
    //        val freqs = fourierFreq(windowLength = y.length, fs = n) // whole signal within one second
    //        // only positive freqs in periodogram since spectrum is symmetric, also ignore DC
    //        // if the periodogram is too noisy, could also be estimating spectral density by, e.g., Welch's method
    //        val periodogram = freqs.toArray.zip(yf.map(cmplx => math.pow(cmplx.abs, 2)).toArray).filter { case (frequency, _) => frequency >= 0 }.tail
    //
    //        // frequency that has peak power in FFT
    //        // equivalent to the number of base periods in the given series
    //        val (pitchFrequency, pitchPower) = periodogram.maxBy(_._2)
    //        println(s"Found $pitchFrequency number of base periods in the given series")
    //        val pitchPowerNormalized = pitchPower * 1/(n - 1)
    //
    ////        val yfDb = yf.map(e => 20*math.log10(e.abs))
    //
    //        // ignored DC -> n - 1
    //        val seriesPower = periodogram.map { case (_, power) => power * 1/(n - 1) }.sum
    //        val relativePitchPower = pitchPowerNormalized / seriesPower
    //        val periodicity = {
    //          if (relativePitchPower >= relativePowerThreshold) {
    //            // having estimated the number of periods in the series, check if there are seven samples in each period
    //            // todo: hourly data could have daily and/or weekly seasonality, too
    //            Some(((n - (n % pitchFrequency)) / pitchFrequency).toInt)
    //          } else None
    //        }
    //        println(s"Series has a periodicity of $periodicity")
    //        periodicity
    //      }
    //      val base = List.fill(3)(List[Double](1, 1, 1, 1, 1, 1.2, 1.2)).flatten
    //      val suffix = List[Double](1, 1)
    //      val all = base ++ suffix
    ////      val all = (1 to 15).map(_ => 1 + 3*math.random.doubleValue()-0.5)
    //
    //      // todo: tests for constant signals
    //      // todo: tests for random signals
    //      val m = periodicityFor(all.toArray)
    //      val a = 1
    //
    //    }
    //  }
  }
}
