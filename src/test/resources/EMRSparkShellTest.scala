/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

/*
For testing inside EMR or other flavors of spark cluster. Run commands after building git repo from source.
Add additional test classes as needed
scala 2.12
spark-shell -i <local_git_repo>/src/test/resources/EMRSparkShellTest.txt \
--packages org.scalatest:scalatest_2.12:3.1.2,org.scalamock:scalamock_2.12:4.4.0,org.scala-lang:scala-compiler:2.12.10,\
org.mockito:mockito-core:2.28.2,org.openjdk.jmh:jmh-core:1.23,org.openjdk.jmh:jmh-generator-annprocess:1.23,org.apache.datasketches:datasketches-java:1.3.0-incubating \
--jars <local_git_repo>/target/deequ_2.12-1.1.0-SNAPSHOT.jar,<local_git_repo>/target/deequ_2.12-1.1.0-SNAPSHOT-tests.jar

scala 2.11
spark-shell -i <local_git_repo>/src/test/resources/EMRSparkShellTest.txt \
--packages org.scalatest:scalatest_2.11:3.1.2,org.scalamock:scalamock_2.11:4.4.0,org.scala-lang:scala-compiler:2.11.10,\
org.mockito:mockito-core:2.28.2,org.openjdk.jmh:jmh-core:1.23,org.openjdk.jmh:jmh-generator-annprocess:1.23,org.apache.datasketches:datasketches-java:1.3.0-incubating \
--jars <local_git_repo>/target/deequ-1.1.0-SNAPSHOT.jar,<local_git_repo>/target/spark-deequ-testing/deequ-1.1.0-SNAPSHOT-tests.jar
 */

import com.amazon.deequ.analyzers.{AnalysisTest, AnalyzerTests, IncrementalAnalysisTest}
import com.amazon.deequ.analyzers.runners.{AnalysisRunnerTests, AnalyzerContextTest}
import com.amazon.deequ.{VerificationResultTest, VerificationSuiteTest}

(new VerificationSuiteTest).execute()
(new VerificationResultTest).execute()
(new AnalysisRunnerTests).execute()
(new AnalyzerContextTest).execute()
(new AnalysisTest).execute()
(new AnalyzerTests).execute()
(new IncrementalAnalysisTest).execute()
//Add additional test classes as needed
