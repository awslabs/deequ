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

package com.amazon.deequ.KLL;

import com.amazon.deequ.analyzers.QuantileNonSample;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class KLLBenchmark {

  private static final int N = 10_000_000;

  private static float[] DATA_FOR_TESTING = createData();

  public static void main(String[] args) throws RunnerException {

    Options opt = new OptionsBuilder()
        .include(KLLBenchmark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }

  private static float[] createData() {
    Random prng = new Random();
    float[] numbers = new float[N];
    for (int i = 0; i < N; i++) {
      numbers[i] = prng.nextFloat();
    }
    return numbers;
  }

  @Benchmark
  public void sumArray(Blackhole bh) {
    float sum = 0.0f;
    for (int i = 0; i < N; i++) {
      sum += DATA_FOR_TESTING[i];
    }
    bh.consume(sum);
  }

  @Benchmark
  public void sketchArrayWithKLL(Blackhole bh) {
    QuantileNonSample<Float> sketch = KLLBenchmarkHelper.floatSketch();
    for (int i = 0; i < N; i++) {
      sketch.update(DATA_FOR_TESTING[i]);
    }
    bh.consume(sketch);
  }

  @Benchmark
  public void sketchArrayWithJavaSketchesKLL(Blackhole bh) {
    KllFloatsSketch sketch = new KllFloatsSketch();
    for (int i = 0; i < N; i++) {
      sketch.update(DATA_FOR_TESTING[i]);
    }
    bh.consume(sketch);
  }
}
