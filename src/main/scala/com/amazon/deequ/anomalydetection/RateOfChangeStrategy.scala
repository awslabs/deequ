package com.amazon.deequ.anomalydetection


/**
  * Provided for backwards compatibility.
  * the old [[RateOfChangeStrategy]] actually detects absolute changes so it has been migrated to [[AbsoluteChangeStrategy]]
  * use [[RelativeRateOfChangeStrategy]] if you want to detect relative changes to the previous values
  */
class RateOfChangeStrategy extends AbsoluteChangeStrategy {
}
