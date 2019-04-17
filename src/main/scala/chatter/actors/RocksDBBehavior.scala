package chatter.actors

import java.io.File
import java.nio.file.{ Files, Paths }

import akka.cluster.Cluster
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import chatter.actors.RocksDBBehavior.{ InitRocksDb, RocksDbReply }
import akka.actor.typed.{ ActorRef, Behavior, ExtensibleBehavior, Signal, TypedActorContext }
import org.rocksdb._

import scala.util.Try
import akka.actor.typed.scaladsl.adapter._

object RocksDBBehavior {

  case class InitRocksDb(replyTo: ActorRef[RocksDbReply])

  case class RocksDbReply(db: RocksDB)

  val name = "rocks-db"
}

//akka://timeline@127.0.0.1:2550/user/rock-db
class RocksDBBehavior(ctx: ActorContext[Unit]) extends ExtensibleBehavior[InitRocksDb] {
  RocksDB.loadLibrary()

  val dbDir = "./" + RocksDBBehavior.name
  Try(Files.createDirectory(Paths.get(s"./$dbDir")))

  val dir = new File(s"${dbDir}/rocks-${Cluster(ctx.asScala.system.toUntyped).selfAddress.port.get}")

  val options = new Options()
    .setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val db = RocksDB.open(options, dir.getPath)

  override def receive(ctx: TypedActorContext[InitRocksDb], msg: InitRocksDb): Behavior[InitRocksDb] =
    msg match {
      case m: InitRocksDb ⇒
        m.replyTo ! RocksDbReply(db)
        Behaviors.same
    }

  override def receiveSignal(
    ctx: TypedActorContext[RocksDBBehavior.InitRocksDb],
    msg: Signal): Behavior[RocksDBBehavior.InitRocksDb] =
    Behaviors.same
}