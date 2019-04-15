package chatter.actors

import java.io.File
import java.nio.file.{ Files, Paths }

import akka.cluster.Cluster
import akka.actor.{ Actor, ActorLogging, Props }
import chatter.actors.RocksDBActor.{ AskRocksDb, RespRocksDb }
import org.rocksdb._
import scala.util.Try

object RocksDBActor {

  case object AskRocksDb

  case class RespRocksDb(db: RocksDB)

  val name = "rock-db"

  def props = Props(new RocksDBActor)
}

class RocksDBActor extends Actor with ActorLogging {
  RocksDB.loadLibrary()

  val dbDir = "./rocks-db"
  Try(Files.createDirectory(Paths.get(s"./$dbDir")))

  val dir = new File(s"${dbDir}/rocks-${Cluster(context.system).selfAddress.port.get}")

  val options = new Options()
    .setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val db = RocksDB.open(options, dir.getPath)

  override def receive: Receive = {
    case AskRocksDb ⇒
      sender() ! RespRocksDb(db)
  }
}