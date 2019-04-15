package akka.cluster.ddata

import java.io.File
import java.nio.file.{ Files, Paths }
import java.sql.DriverManager
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging }
import akka.cluster.Cluster
import akka.cluster.ddata.DurableStore.{ DurableDataEnvelope, LoadAll, LoadAllCompleted, LoadData, LoadFailed, Store }
import akka.serialization.{ SerializationExtension, SerializerWithStringManifest }
import chatter.actors.typed.ChatTimelineReplicator
import chatter.crdt.ChatTimeline
import com.typesafe.config.Config

import scala.util.control.NonFatal
import scala.util.Try

final class H2DurableStore(config: Config) extends Actor with ActorLogging {
  Class.forName("org.h2.Driver")

  val dbDir = "h2-db"
  val cluster = Cluster(context.system)

  Try(Files.createDirectory(Paths.get(s"./$dbDir")))

  val CreateTable = "CREATE TABLE IF NOT EXISTS KVS(key varchar primary key, replica varchar(255), value blob)"

  val InsertQuery = "INSERT INTO KVS(key, replica, value) values (?,?,?)"

  val SelectQuery = "SELECT key, value FROM KVS WHERE replica = ?"

  val DeleteQuery = "DELETE from KVS WHERE key = ? AND replica = ?"

  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[DurableDataEnvelope]).asInstanceOf[SerializerWithStringManifest]
  val manifest = serializer.manifest(new DurableDataEnvelope(Replicator.Internal.DeletedData))

  val dataDir = config.getString("h2.dir") match {
    case path if path.endsWith("ddata") ⇒
      new File("db-" + cluster.selfAddress.port.get.toString)
    case path ⇒
      new File(path)
  }

  val con = DriverManager.getConnection(s"jdbc:h2:./${dbDir}/$dataDir;DB_CLOSE_DELAY=-1", "sa", "qwerty")

  con.prepareStatement(CreateTable).executeUpdate

  val prepSelect = con.prepareStatement(SelectQuery)

  val prepDelete = con.prepareStatement(DeleteQuery)

  val prepInsert = con.prepareStatement(InsertQuery)

  val segments = self.path.elements.toSeq

  val replicaName = segments.find(_.contains(ChatTimelineReplicator.postfix))
    .getOrElse(throw new Exception("Couldn't find needed segment"))

  override def postStop(): Unit = {
    log.error("postStop")
    con.close
  }

  def active: Receive = {
    case Store(key, data, reply) ⇒
      try {
        //delete prev
        prepDelete.setString(1, key)
        prepDelete.setString(2, replicaName)
        prepDelete.execute //== 1

        //insert new
        val valueBts = serializer.toBinary(data)
        prepInsert.setString(1, key)
        prepInsert.setString(2, replicaName)
        prepInsert.setBytes(3, valueBts)
        prepInsert.execute
        reply.foreach { sr ⇒ sr.replyTo ! sr.successMsg }
      } catch {
        case NonFatal(e) ⇒
          log.error(e, "Failed to store [{}]", key)
          reply.foreach { sr ⇒ sr.replyTo ! sr.failureMsg }
      }
  }

  def init: Receive = {
    case LoadAll ⇒
      val startTs = System.nanoTime
      var savedResult = Map.empty[String, DurableDataEnvelope]
      try {
        prepSelect.setString(1, replicaName)
        val rs = prepSelect.executeQuery
        while (rs.next) {
          val key = rs.getString("key")
          val bts = rs.getBytes("value")
          val envelope = serializer.fromBinary(bts, manifest).asInstanceOf[DurableDataEnvelope]
          log.info("Load {} [{} - {}] size:{}", replicaName, key, envelope.data.asInstanceOf[ChatTimeline].timeline.size, bts.size)
          savedResult = savedResult + (key -> envelope)
        }

        if (savedResult.nonEmpty) {
          log.info("Load all keys [{}] took [{} ms]", savedResult.keySet.mkString(","),
                                                      TimeUnit.NANOSECONDS.toMillis(System.nanoTime - startTs))
          sender() ! LoadData(savedResult)
        }

        sender() ! LoadAllCompleted
        context become active
      } catch {
        case NonFatal(e) ⇒
          throw new LoadFailed("failed to load durable ddata store", e)
      }
  }

  override def receive: Receive = init
}
