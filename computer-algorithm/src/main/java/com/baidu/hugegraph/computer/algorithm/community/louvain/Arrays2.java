/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.computer.algorithm.community.louvain;

import java.util.Arrays;
import java.util.Random;

public class Arrays2 {
    public static double calcSum(double[] value) {
        double sum;
        int i;

        sum = 0;
        for (i = 0; i < value.length; i++)
            sum += value[i];
        return sum;
    }

    public static double calcSum(double[] value, int beginIndex, int endIndex) {
        double sum;
        int i;

        sum = 0;
        for (i = beginIndex; i < endIndex; i++)
            sum += value[i];
        return sum;
    }

    public static double calcAverage(double[] value) {
        double average;
        int i;

        average = 0;
        for (i = 0; i < value.length; i++)
            average += value[i];
        average /= value.length;
        return average;
    }

    public static double calcMedian(double[] value) {
        double median;
        double[] sortedValue;

        sortedValue = (double[])value.clone();
        Arrays.sort(sortedValue);
        if (sortedValue.length % 2 == 1)
            median = sortedValue[(sortedValue.length - 1) / 2];
        else
            median = (sortedValue[sortedValue.length / 2 - 1] +
                    sortedValue[sortedValue.length / 2]) / 2;
        return median;
    }

    public static double calcMinimum(double[] value) {
        double minimum;
        int i;

        minimum = value[0];
        for (i = 1; i < value.length; i++)
            minimum = Math.min(minimum, value[i]);
        return minimum;
    }

    public static double calcMaximum(double[] value) {
        double maximum;
        int i;

        maximum = value[0];
        for (i = 1; i < value.length; i++)
            maximum = Math.max(maximum, value[i]);
        return maximum;
    }

    public static int calcMaximum(int[] value) {
        int i, maximum;

        maximum = value[0];
        for (i = 1; i < value.length; i++)
            maximum = Math.max(maximum, value[i]);
        return maximum;
    }

    public static int[] generateRandomPermutation(int nElements) {
        return generateRandomPermutation(nElements, new Random());
    }

    public static int[] generateRandomPermutation(int nElements,Random random) {
        int i, j, k;
        int[] permutation;

        permutation = new int[nElements];
        for (i = 0; i < nElements; i++)
            permutation[i] = i;
        for (i = 0; i < nElements; i++) {
            j = random.nextInt(nElements);
            k = permutation[i];
            permutation[i] = permutation[j];
            permutation[j] = k;
        }
        return permutation;
    }
}
