package io.datamass

import org.apache.spark.SparkContext


object Driver {

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      System.err.println("Usage: WordCount <file-hdfs>")
      System.exit(1)
    }

    val sc = new SparkContext()

    val counts = sc.textFile(args(0))
      .flatMap(line => line.split("\t"))
      .map(word => (word,1))
      .reduceByKey(_+_)

    counts.take(5).foreach(println)

  }

}
