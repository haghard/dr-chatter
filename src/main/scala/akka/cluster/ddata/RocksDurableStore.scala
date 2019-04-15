package akka.cluster.ddata

import java.util.concurrent.{ ThreadLocalRandom, TimeUnit }

import akka.cluster.ddata.DurableStore._
import akka.actor.{ Actor, ActorLogging, RootActorPath, Stash }
import akka.serialization.{ SerializationExtension, SerializerWithStringManifest }
import akka.util.ByteString
import com.typesafe.config.Config
import org.rocksdb.RocksDB
import org.rocksdb._
import chatter.actors.RocksDBActor
import chatter.actors.typed.ChatTimelineReplicator
import chatter.crdt.ChatTimeline

import scala.util.control.NonFatal

/*
https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBColumnFamilySample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBSample.java
*/

class RocksDurableStore(config: Config) extends Actor with ActorLogging with Stash {
  RocksDB.loadLibrary()

  val SEPARATOR = '.'
  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[DurableDataEnvelope]).asInstanceOf[SerializerWithStringManifest]
  val manifest = serializer.manifest(new DurableDataEnvelope(Replicator.Internal.DeletedData))

  val rocksWriteOpts = new WriteOptions()
    .setSync(true)
    .setDisableWAL(false)

  val options = new Options()
    .setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val segments = self.path.elements.toSeq
  val replicaName = segments.find(_.contains(ChatTimelineReplicator.postfix))
    .getOrElse(throw new Exception("Couldn't find needed segment"))

  val flushOps = new FlushOptions().setWaitForFlush(true)

  def awaitDB: Receive = {
    val path = RootActorPath(context.self.path.address) / "user" / RocksDBActor.name //"rocks-db"
    context.actorSelection(path) ! RocksDBActor.AskRocksDb

    {
      case RocksDBActor.RespRocksDb(db) ⇒
        unstashAll()
        context.become(load(db))
      case _ ⇒
        stash()
    }
  }

  def load(db: RocksDB): Receive = {
    case LoadAll ⇒
      val ts = System.nanoTime()
      var savedResult = Map.empty[String, DurableDataEnvelope]
      var iter: RocksIterator = null
      try {
        iter = db.newIterator
        iter.seekToFirst
        while (iter.isValid) {
          val keyWithReplica = new String(iter.key, ByteString.UTF_8)
          val bts = iter.value
          val envelope = serializer.fromBinary(bts, manifest).asInstanceOf[DurableDataEnvelope]

          //chat-1.betta-repl
          if (keyWithReplica.endsWith(replicaName)) {
            val segments = keyWithReplica.split(SEPARATOR)
            val originalKey = segments(0)

            log.info("Load [{} - {}] size:{}", originalKey,
                                               envelope.data.asInstanceOf[ChatTimeline].timeline.size, bts.size)

            savedResult = savedResult + (originalKey -> envelope)
          }
          iter.next
        }

        if (savedResult.nonEmpty) {
          log.info("Load all keys [{}] took [{} ms]", savedResult.keySet.mkString(","),
                                                      TimeUnit.NANOSECONDS.toMillis(System.nanoTime - ts))
          sender() ! LoadData(savedResult)
        }

        sender() ! LoadAllCompleted
        context become active(db)
      } catch {
        case NonFatal(e) ⇒
          throw new LoadFailed("failed to load durable ddata store", e)
      } finally {
        if (iter ne null)
          iter.close
      }
  }

  def active(db: RocksDB): Receive = {
    case Store(key, data, reply) ⇒
      try {
        val keyWithReplica = (key + SEPARATOR + replicaName)
        val keyBts = keyWithReplica.getBytes(ByteString.UTF_8)
        val valueBts = serializer.toBinary(data)

        if (ThreadLocalRandom.current.nextDouble > .97)
          log.warning("write key: {}", keyWithReplica)

        db.put(rocksWriteOpts, keyBts, valueBts)
        db.flush(flushOps) //for durability
        reply.foreach { r ⇒ r.replyTo ! r.successMsg }
      } catch {
        case NonFatal(e) ⇒
          log.error(e, "Failed to store [{}]", key)
          reply.foreach { r ⇒ r.replyTo ! r.failureMsg }
      }
  }

  override def receive: Receive = awaitDB
}