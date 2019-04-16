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

  val name = "rocks-db"

  def props = Props(new RocksDBActor)
}

//akka://dr-chatter@127.0.0.1:2550/user/rock-db
class RocksDBActor extends Actor with ActorLogging {
  RocksDB.loadLibrary()

  val dbDir = "./" + RocksDBActor.name
  Try(Files.createDirectory(Paths.get(s"./$dbDir")))

  val dir = new File(s"${dbDir}/rocks-${Cluster(context.system).selfAddress.port.get}")

  val options = new Options()
    .setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val db = RocksDB.open(options, dir.getPath)

  //log.warning("RocksDBActor")

  override def receive: Receive = {
    case AskRocksDb ⇒
      sender() ! RespRocksDb(db)
  }
}