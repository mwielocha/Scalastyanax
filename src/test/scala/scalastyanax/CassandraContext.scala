package scalastyanax

import _root_.me.prettyprint.hector.api.factory.HFactory
import org.specs2.specification.{BeforeEach, Context, Apply, Scope}
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{Cluster, AstyanaxContext}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{LongSerializer, StringSerializer}
import scala.collection.JavaConversions._
import org.specs2.mutable.{After, BeforeAfter, Before, Around}
import org.specs2.execute.{AsResult, Result}
import org.slf4j.LoggerFactory
import com.google.common.collect.ImmutableMap
import org.apache.cassandra.service.CassandraDaemon
import com.netflix.astyanax.test.EmbeddedCassandra
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 22.08.2013
 * Time: 21:01
 * To change this template use File | Settings | File Templates.
 */

object CassandraConnection {

  final val DEFAULT_CASSANDRA_PORT = 9162
  final val DEFAULT_CASSANDRA_STORAGE_PORT = 7002
  final val DEFAULT_CLUSTER_NAME = "ConnectionPool_Cluster"

  val keyspaceName = "Scalastyanax_unit"
  val columnFamilyName = "Scalastyanax_unit_cf"
  val columnFamilyWithCounterColumnsName = "Scalastyanax_counter_unit_cf"

  lazy val embeddedDb = {
    val tempCassadraStorageDir: File = new File("/tmp/cassandra_" + System.currentTimeMillis)
    tempCassadraStorageDir.deleteOnExit()
    val cass = new EmbeddedCassandra(tempCassadraStorageDir,
      DEFAULT_CLUSTER_NAME,
      DEFAULT_CASSANDRA_PORT,
      DEFAULT_CASSANDRA_STORAGE_PORT)
    cass
  }

  def startDb = {
    embeddedDb.start()
    val cluster = HFactory.getOrCreateCluster("ConnectionPool_Cluster", s"localhost:$DEFAULT_CASSANDRA_PORT")
    cluster.addKeyspace(HFactory.createKeyspaceDefinition(keyspaceName))
  }

  def stopDb = {
    embeddedDb.stop()
  }

}

class CassandraConnection(clusterName: String, clusterHost: String, clusterPort: Int) {

  import CassandraConnection._

  lazy val connectionPool = new ConnectionPoolConfigurationImpl(clusterName)
    .setMaxConnsPerHost(80)
    .setSeeds(s"$clusterHost:$clusterPort")
    .setMaxOperationsPerConnection(10000)
    .setMaxPendingConnectionsPerHost(20)
    .setConnectionLimiterMaxPendingCount(20)
    .setTimeoutWindow(10000)
    .setConnectionLimiterWindowSize(2000)
    .setMaxTimeoutCount(3)
    .setConnectTimeout(100)
    .setConnectTimeout(2000)
    .setMaxFailoverCount(-1)
    .setLatencyAwareBadnessThreshold(20)
    .setLatencyAwareUpdateInterval(1000) // 10000
    .setLatencyAwareResetInterval(10000) // 60000
    .setLatencyAwareWindowSize(100) // 100
    .setLatencyAwareSentinelCompare(100f)
    .setSocketTimeout(30000)
    .setMaxTimeoutWhenExhausted(10000)
    .setInitConnsPerHost(10)

  lazy val cassandraContext: AstyanaxContext[Cluster] = new AstyanaxContext.Builder()
    .forCluster(clusterName)
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
    .setDiscoveryType(NodeDiscoveryType.NONE)
    )
    .withConnectionPoolConfiguration(connectionPool)
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildCluster(ThriftFamilyFactory.getInstance())

  cassandraContext.start()

  val cluster: Cluster = cassandraContext.getClient

  implicit lazy val keyspace = cluster.getKeyspace(keyspaceName)

  implicit lazy val columnFamily = new ColumnFamily[String, String](
    columnFamilyName, // Column Family Name
    StringSerializer.get(), // Key Serializer
    StringSerializer.get(), // Column Serializer
    StringSerializer.get()) // Value Serializer

  implicit lazy val columnFamilyWithCounterColumns = new ColumnFamily[String, String](
    columnFamilyWithCounterColumnsName, // Column Family Name
    StringSerializer.get(), // Key Serializer
    StringSerializer.get(), // Column Serializer
    LongSerializer.get()) // Value Serializer
}

class CassandraContext extends After {

  import scalastyanax.Scalastyanax._
  import CassandraConnection._

  val logger = LoggerFactory.getLogger(getClass)

  val connection = new CassandraConnection(DEFAULT_CLUSTER_NAME, "localhost", DEFAULT_CASSANDRA_PORT)

  implicit val keyspace = connection.keyspace
  implicit val columnFamily = connection.columnFamily
  implicit val columnFamilyWithCounterColumns = connection.columnFamilyWithCounterColumns

  columnFamily.create(
    "default_validation_class" -> "UTF8Type",
    "key_validation_class" -> "UTF8Type",
    "comparator_type" -> "UTF8Type"
  )

  columnFamilyWithCounterColumns.create(
    "default_validation_class" -> "CounterColumnType",
    "key_validation_class" -> "UTF8Type",
    "comparator_type" -> "UTF8Type"
  )

  def after: Any = {
    logger.info("Clean up...")
    columnFamily.truncate
    columnFamilyWithCounterColumns.truncate
  }
}
