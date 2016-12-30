package zz.sample

import java.math.BigInteger
import java.nio.charset.Charset
import java.util.UUID

import com.google.common.hash.{HashCode, Hashing}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

/**
  * Created by zulk on 25.12.16.
  */
class PregelTestCls extends Serializable{

  def process() = {
    val ss = SparkSession.builder().appName("pregel").master("local[*]").getOrCreate()
//    ss.catalog.listDatabases().show(false)
    import ss.implicits._

    val assets = Array(
      (1L,Set("Cl1")),
      (2L,Set("Cl2")),
      (3L,Set("Cl3")),
      (4L,Set("Cl4")),
      (5L,Set("Cl5")),
      (6L,Set("Cl6")),
      (7L,Set("Cl7")),
      (8L,Set("Cl8"))
    )

    val v:RDD[(VertexId,Set[String])]  = ss.sparkContext.parallelize(assets)

    val edges = Array(
      Edge(1L,2L,"E1"),
      Edge(2L,3L,"E2"),
      Edge(5L,3L,"E2"),
      Edge(3L,4L,"E2")

//      Edge(4L,2L,"E3"),
//      Edge(5L,6L,"E4"),
//      Edge(7L,8L,"E5")
    )
    val e = ss.sparkContext.parallelize(edges)

    val g = Graph(v,e)

    g.pregel[Set[String]](Set(),Int.MaxValue,EdgeDirection.Out)(
      vprog = (id, v, msg) => {
        if (msg.isEmpty) {
//          println(s"->msg [$msg] v[$v] id[$id]")
          v
        } else {
//          println(s"-->msg [$msg] v[$v] id[$id]")
          msg | v
        }
      },
      t => {
//          println(s"-->dst[${t.dstId}] msg[${t.srcAttr}] from[${t.srcId}]")
          Iterator((t.dstId, t.srcAttr))
      },
    (a,b) => a|b
    ).vertices.join(g.vertices).
      map {
        case(id,joined) => (id,(joined._1 diff joined._2).toList)
      }.toDS().withColumn("aaa",lit(1)).show(false)
//    GraphFrame.fromGraphX(g).vertices.show()
  }

  private def processV(v: String, msg: String) = {
  }
}


object PregelTest extends App {
  val  p = new PregelTestCls()
  p.process()
}
