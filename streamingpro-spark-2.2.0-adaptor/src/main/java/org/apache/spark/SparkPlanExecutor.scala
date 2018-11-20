package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ReuseSubquery, SparkPlan}

object SparkPlanExecutor {

  def exec(plan: SparkPlan, sparkSession: SparkSession) = {
    doExec(plan)
  }

  def firstPartition(rdd: RDD[InternalRow]): Partition = {
    rdd.partitions.head
  }

  def doExec(sparkPlan: SparkPlan): List[InternalRow] = {
    val rdd = sparkPlan.execute().map(ite => ite.copy())
    print("--------"+rdd.count())
    val partition = firstPartition(rdd)
    rdd.compute(partition, new OptimizeTaskContext).toList
  }

  def rddCompute(rdd: RDD[InternalRow]): List[InternalRow] = {
    val partition = firstPartition(rdd)
    rdd.compute(partition, new OptimizeTaskContext).toList
  }

}
