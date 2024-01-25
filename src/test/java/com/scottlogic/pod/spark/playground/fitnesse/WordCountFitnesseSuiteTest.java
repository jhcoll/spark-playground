package com.scottlogic.pod.spark.playground.fitnesse;

import org.junit.runner.RunWith;

import fitnesse.junit.FitNesseRunner;
import fitnesse.junit.FitNesseRunner.FitnesseDir;
import fitnesse.junit.FitNesseRunner.OutputDir;
import fitnesse.junit.FitNesseRunner.Suite;

@RunWith(FitNesseRunner.class)
@Suite("MyTestSuite.WordCountSuiteTest")
@FitnesseDir("./FitNesse")
@OutputDir("./target/fitnesse-results")
public class WordCountFitnesseSuiteTest {
}
