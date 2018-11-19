package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.storage.StorageLevel
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.session.MLSQLException


class SQLCacheExt(override val uid: String) extends SQLAlg with WowParams {

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val exe = params.get(execute.name).getOrElse {
      "cache"
    }
    val repl = params.get(replicate.name).map(f => f.toInt).getOrElse {
      2
    }
    if (!execute.isValid(exe)) {
      throw new MLSQLException(s"${execute.name} should be cache or uncache")
    }

    if (exe == "cache") {
      df.persist(StorageLevel(false, true, false, true, repl))
    } else {
      df.unpersist()
    }
    df
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("train is not support")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  final val execute: Param[String] = new Param[String](this, "execute", "cache|uncache", isValid = (m: String) => {
    m == "cache" || m == "uncache"
  })

  final val replicate: Param[Int] = new Param[Int](this, "replicate", "")

  def this() = this(BaseParams.randomUID())
}
