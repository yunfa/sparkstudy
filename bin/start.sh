#!/bin/bash
SPARK_HOME=/usr/software/spark2

$SPARK_HOME/bin/spark-submit --properties-file ../spark2/conf/spark-defaults.conf --driver-cores 2 --master spark://local:7077 --name "first spark app" --class me.yunfa.spark.demo.lesson3.Demo01HelloSpark myspark.jar