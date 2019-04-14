package chatter.actors

import java.io.File
import java.nio.file.{ Files, Paths }

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.Cluster
import chatter.actors.RocksDBActor.{ AskRocksDb, RespRocksDb }
import org.rocksdb._

import scala.util.Try

object RocksDBActor {

  case object AskRocksDb
  case object StopRocksDb
  case class RespRocksDb(db: RocksDB)

  val name = "rock-db"

  def props = Props(new RocksDBActor)
}

class RocksDBActor extends Actor with ActorLogging {
  RocksDB.loadLibrary()

  val dbDir = "./rocks-db"
  Try(Files.createDirectory(Paths.get(s"./$dbDir")))

  val dir = new File(s"${dbDir}/rocks-${Cluster(context.system).selfAddress.port.get}")

  val writeOpts = new WriteOptions()
    .setSync(true)
    .setDisableWAL(false)

  val options = new Options()
    .setCreateIfMissing(true)
    .setMaxBackgroundCompactions(10)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val db = RocksDB.open(options, dir.getPath)

  //var replicas: Set[ActorRef] = Set.empty

  override def postStop(): Unit = {
    //replicas.foreach(_ ! )
  }

  override def receive: Receive = {
    case AskRocksDb ⇒
      //replicas = replicas + sender()
      sender() ! RespRocksDb(db)
  }
}