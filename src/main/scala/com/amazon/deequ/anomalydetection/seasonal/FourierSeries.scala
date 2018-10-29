package com.amazon.deequ.anomalydetection.seasonal;

case class FourierSeries(mode: FourierSeries.DetectionMode.Value, metricInterval: FourierSeries.MetricInterval.Value) extends AnomalyDetectionStrategy {
  import FourierSeries._

  /**
    * Search for anomalies in a series of data points.
    *
    * @param dataSeries     The data contained in a Vector of Doubles
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return The indices of all anomalies in the interval and their corresponding wrapper object.
    */
  override def detect(dataSeries: Vector[Double], searchInterval: (Int, Int)): Seq[(Int, Anomaly)] = {
    val (start, end) = searchInterval
    require(start <= end, "TODO")

    // todo: only fit spectrum for non search interval (slice)
    val y = DenseVector(dataSeries.toArray)

    val spectrum = modelSelection(y)
    println(s"Using spectrum with ${spectrum.data.map(c => if (c.real != 0) 1 else 0).sum} non zero frequencies")

    // todo: forecast the whole search interval and flag everything as an anomaly that does not fit training set stats
    val fc = (0 until 2*y.length).map(t => timeDomainFor(t, spectrum)/y.length)

    // detection
    // percentiles here
    val x = DenseVector(fc.take(y.length).toArray) - DenseVector(dataSeries.toArray)
    val p95 = DescriptiveStats.percentile(x.valuesIterator, p = 0.95)
    val p05 = DescriptiveStats.percentile(x.valuesIterator, p = 0.05)

    Seq.empty
  }
}
