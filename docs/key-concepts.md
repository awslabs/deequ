# Key Concepts in the Codebase
There are a few key concepts that will help you to understand the codebase.

## Metrics, Analyzers, and State
Metrics represent some metric associated with the data that changes over time. For example counting the rows in a
DataFrame.

An Analyzer knows how to calculate a Metric based on some input DataFrame.

State is an optimization - it represents the state of the data, from which a metric can be calculated. This intermediate
state can then be used to calculate future metrics more quickly. Check out the examples for some further details.

## Overall flow of running deequ checks
When running checks a user specifies a DataFrame and a number of checks to do on that DataFrame. Many checks in Deequ
are based on metrics which describe the data. In order to perform the checks the user requests deequ follows the
following process:
* First deequ figures out which Analyzers are required
* Metrics are calculated using those Analyzers
** Metrics are also stored if a MetricsRepository is provided 
** Intermediate state is stored if a StatePersister is provided
** Intermediate state is used for metric calculations if a StateLoader is provided
* Checks are evaluated using the calculated Metrics

The reason it works this way is for performance, primarily because calculating metrics at the same time gives the
opportunity to calculate them in fewer passes over the data. 

### Analyzers
Types of analyzers:
* ScanShareableAnalyzer - an analyzer which computes a metric based on a straight scan over the data, without any
grouping being required
* GroupingAnalyzer - an analyzer that requires the data to be grouped by a set of columns before the metric can be
calculated

### Metrics
A metric includes the following key details
* name - the name for the type of metric
* entity - the type of entity the metric is recorded against. e.g. A column, dataset, or multicolumn
* instance - information about this instance of the metric. For example this could be the column name the metric is
operating on
* value - the value of the metric at a point in time. The type of this value varies between metrics.

#### Metrics storage
Metrics can be stored in a metrics repository. An entry in the repository consists of:
* A resultKey, which is a combination of a timestamp and a map of tags. Typically a user may want to record things
like the data source (e.g. table name) with the tags. The resultKey can be used to lookup stored metrics
* An analyzerContext, which consists of a map of Analyzers to Metrics

### State
Please consult the examples or the codebase for more details on State. 