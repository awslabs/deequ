## ApproxCountDistinct
    Compute approximated count distinct with [HyperLogLogPlusPlus](https://en.wikipedia.org/wiki/HyperLogLog#HLL++)
## ApproxQuantile
    Approximate quantile analyzer. 
    Yields the x-quantile; a value, for which it holds true, that exaclty x percent of the total values are smaller than the yielded value.
    The quantile x can be passed to the analyzer as parameter, x has to be a number between 0 and 1. Choosing 0.5 will yield the median.
    The allowed relative error compared to the exact quantile can be configured with `relativeError` parameter. A `relativeError` = 0.0 would yield the exact quantile while increasing the computational load.
## ApproxQuantiles
    The same as ApproxQuantile, can be given a sequence of quantiles.
## Completeness
    Completeness is the fraction of the number of non-null values devided by the number of all values in a column.
## Compliance
    Compliance is a measure of the fraction of rows that complies with the given column constraint.
    E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
    3 and 10 rows under 3; a DoubleMetric would be returned with 0.33 value
## Correlation
## CountDistinct
## DataType
## Distinctness
## Entropy
## Histogram
## Maximum
## Mean
## Median
## Minimum
## Mode
## MutualInformation
## PatternMatch
    Gives the fraction of values that match a certain regex constraint divided by all values in the given column.
## Size
    Is the amount of values in the given column.
## StandardDeviation
    Quantifies the amount of variation of the values in the given column. A low standard deviation means that the values are close to the mean, while a high standard deviation means that they are spread out over a large value range. It is calculated by taking the square root of the variance.
## Sum
    The sum of all values in the given column.
## UniqueValueRatio
    The quotient of all unique values divided by all distinct columns of the given column. The unique values only appear once in the column. The distinct values are all different values in the column where every value is counted once.
## Uniqueness
    Gives the fraction of values of the given column that only appear once in the whole column divided by the Size of the column.