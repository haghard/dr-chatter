package chatter.actors

import java.time.ZoneId

import akka.cluster.Cluster
import akka.routing.ConsistentHashingGroup
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{ Actor, ActorLogging, ActorSystem, Props }
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
import chatter.actors.ChatTimelineReader.{ AllShards, AskForShards }
import chatter.{ LocalShard, RemoteShard, Shard }

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.util.Random

object ChatTimelineWriter {

  sealed trait WriteProtocol

  case class Write(chatName: String, when: Long, tz: String, authorId: Long, content: String) extends WriteProtocol

  case class WriteSuccess(chatName: String) extends WriteProtocol

  case class WriteFailure(chatName: String, errorMsg: String) extends WriteProtocol

  case class WriteTimeout(chatName: String) extends WriteProtocol

  sealed trait ReadProtocol

  case class ReadLocalChatTimeline(chatName: String) extends ReadProtocol

  case class ReadRemoteChatTimeline(chatName: String) extends ReadProtocol

  def routeeName(shard: String) = s"$shard-replicator"

  def props(system: ActorSystem, localShards: Vector[String], proxy: String, ids: Seq[Int]) =
    Props(new ChatTimelineWriter(system, localShards, proxy, ids))
}

class ChatTimelineWriter(system: ActorSystem, localShards: Vector[String], proxy: String, ids: Seq[Int]) extends Actor
  with ActorLogging with akka.actor.Timers {

  import ChatTimelineWriter._

  implicit val ex = context.dispatcher
  implicit val cluster = Cluster(context.system)

  val address = cluster.selfUniqueAddress.address

  val rnd = new Random(System.currentTimeMillis)

  val shards: Vector[Shard] = {
    val zero = new TreeSet[Shard]()((a: Shard, b: Shard) ⇒ a.name.compareTo(b.name))
    localShards.foldLeft(zero)(_ + createLocalReplicator(_)) + createProxy(proxy)
  }.toVector

  val rocksDBActor = system.actorOf(RocksDBActor.props, RocksDBActor.name)

  override def preStart(): Unit = {
    timers.startSingleTimer(self.path.toString, 'StartWriting, 3.second)
  }

  def createLocalReplicator(shard: String) = {
    //supervision ???
    val name = routeeName(shard)
    log.info("★ ★ ★  local replicator for {}", shard)
    LocalShard(shard, system.actorOf(ChatTimelineReplicator.props(system, shard), name))
  }

  //we don't allocate anything just return an actorRef on existing actor
  def createProxy(shard: String) = {
    val shardName = routeeName(shard)
    log.info("★ ★ ★  remote replicator for {}", shard)
    val remoteProxyRouter = system.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances    = localShards.size,
          routeesPaths      = List(s"/user/$shardName"),
          allowLocalRoutees = false, //important
          useRoles          = shard)
      ).props(), s"router-$shard")
    RemoteShard(shard, remoteProxyRouter)
  }

  def awaitStart: Receive = {
    case AskForShards ⇒
      sender() ! AllShards(shards)
    case 'StartWriting ⇒
      write(1l)
  }

  def write(n: Long) = {
    val chatId = ids(ThreadLocalRandom.current.nextInt(ids.size))
    val shard = shards(chatId % shards.size)

    val userId = ThreadLocalRandom.current.nextLong(0l, 10l)
    val msg = Write(s"chat-$chatId", System.currentTimeMillis, ZoneId.systemDefault.getId, userId, rnd.nextString(1024 * 1))
    shard match {
      case LocalShard(_, ref) ⇒
        ref ! msg
      case RemoteShard(_, ref) ⇒
        ref ! ConsistentHashableEnvelope(message = msg, hashKey = chatId)
    }
    context.become(awaitResponse(n))
  }

  def awaitResponse(n: Long): Receive = {
    case AskForShards ⇒
      sender() ! AllShards(shards)
      log.warning("★ ★ ★  stop writer ★ ★ ★")
      context.stop(self)

    case WriteSuccess(_) ⇒
      write(n + 1l)

    /*if (context.system.scheduler ne null)
        context.system.scheduler.scheduleOnce(10.millis, new Runnable {
          override def run = write(n + 1l)
        })*/

    case WriteFailure(chatName, errorMsg) ⇒
      log.error(s"WriteFailure into ${chatName} because of ${errorMsg}")
      write(n + 1l)
    case WriteTimeout(chatName) ⇒
      log.error(s"WriteTimeout into ${chatName}")
      write(n + 1l)
  }

  override def receive: Receive = awaitStart
}