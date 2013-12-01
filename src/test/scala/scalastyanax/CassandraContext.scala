package scalastyanax

import org.specs2.specification.{BeforeEach, Context, Apply, Scope}
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{Cluster, AstyanaxContext}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import scala.collection.JavaConversions._
import org.specs2.mutable.{After, BeforeAfter, Before, Around}
import org.specs2.execute.{AsResult, Result}
import org.slf4j.LoggerFactory
import com.google.common.collect.ImmutableMap
import org.apache.cassandra.service.CassandraDaemon

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 22.08.2013
 * Time: 21:01
 * To change this template use File | Settings | File Templates.
 */
object CassandraConnection {

  lazy val defaultConnection = new CassandraConnection

}

class CassandraConnection {
  val keyspaceName = "Scalastyanax_unit"
  val columnFamilyName = "Scalastyanax_unit_cf"

  lazy val connectionPool = new ConnectionPoolConfigurationImpl("ConnectionPool_Cluster")
    .setMaxConnsPerHost(80)
    .setSeeds("localhost:9160")
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
    .forCluster("Cluster")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
    .setDiscoveryType(NodeDiscoveryType.NONE)
  )
    .withConnectionPoolConfiguration(connectionPool)
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildCluster(ThriftFamilyFactory.getInstance());

  cassandraContext.start()

  val cluster: Cluster = cassandraContext.getClient

  lazy val keyspace = cluster.getKeyspace(keyspaceName)

  lazy val columnFamily = new ColumnFamily[String, String](
    columnFamilyName, // Column Family Name
    StringSerializer.get(), // Key Serializer
    StringSerializer.get(), // Column Serializer
    StringSerializer.get()) // Value Serializer
}

class CassandraContext extends After {

  val logger = LoggerFactory.getLogger(getClass)

  import CassandraConnection._

  if(defaultConnection.keyspace.describeKeyspace().getColumnFamily(defaultConnection.columnFamilyName) == null) {
    defaultConnection.keyspace.createColumnFamily(columnFamily, ImmutableMap.builder[String, Object]()
      .put("default_validation_class", "UTF8Type")
      .put("key_validation_class",     "UTF8Type")
      .put("comparator_type",          "UTF8Type").build())
  }

  implicit val columnFamily = defaultConnection.columnFamily
  implicit val keyspace = defaultConnection.keyspace

  def after: Any = {
    logger.info("Clean up...")
    keyspace.truncateColumnFamily(columnFamily)
  }
}
