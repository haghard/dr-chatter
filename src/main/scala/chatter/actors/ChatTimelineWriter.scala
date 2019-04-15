package chatter
package actors

import java.time.ZoneId

import akka.cluster.Cluster
import akka.routing.ConsistentHashingGroup
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import chatter.actors.ChatTimelineReader.AllShards

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.util.Random
import akka.actor.typed.scaladsl.adapter._
import chatter.actors.typed.ChatTimelineReplicator

object ChatTimelineWriter {

  sealed trait RResponses

  sealed trait ReplicatorProtocol

  case object AskForShards extends ReplicatorProtocol

  case class WriteChatTimeline(chatName: String, when: Long, tz: String, authorId: Long, content: String) extends ReplicatorProtocol
  case class ReadLocalChatTimeline(chatName: String) extends ReplicatorProtocol
  case class ReadRemoteChatTimeline(chatName: String) extends ReplicatorProtocol

  case class WriteSuccess(chatName: String) extends ReplicatorProtocol with RResponses
  case class WriteFailure(chatName: String, errorMsg: String) extends ReplicatorProtocol with RResponses
  case class WriteTimeout(chatName: String) extends ReplicatorProtocol with RResponses

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

  val shards: Vector[Shard0[ReplicatorProtocol]] = {
    val zero = new TreeSet[Shard0[ReplicatorProtocol]]()((a: Shard0[ReplicatorProtocol], b: Shard0[ReplicatorProtocol]) ⇒ a.name.compareTo(b.name))
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
    val a: akka.actor.typed.ActorRef[ReplicatorProtocol] =
      system.spawn(ChatTimelineReplicator(system, shard, self.toTyped[RResponses]), name)
    LocalShard0(shard, a)
    //LocalShard(shard, system.actorOf(ChatTimelineReplicator.props(system, shard), name))
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

    import akka.actor.typed.scaladsl.adapter._
    RemoteShard0(shard, remoteProxyRouter.toTyped[ReplicatorProtocol])
    //RemoteShard(shard, remoteProxyRouter)
  }

  def awaitStart: Receive = {
    case AskForShards ⇒
    //sender() ! AllShards(shards)
    case 'StartWriting ⇒
      write(1l)
  }

  def write(n: Long) = {
    val chatId = ids(ThreadLocalRandom.current.nextInt(ids.size))
    val shard = shards(chatId % shards.size)

    val userId = ThreadLocalRandom.current.nextLong(0l, 10l)

    val msg = WriteChatTimeline(s"chat-$chatId", System.currentTimeMillis, ZoneId.systemDefault.getId,
                                                 userId, rnd.nextString(1024 * 1))
    import akka.actor.typed.scaladsl.adapter._
    shard match {
      case LocalShard0(_, ref) ⇒
        ref ! msg
      case RemoteShard0(_, ref) ⇒
        ref.toUntyped ! ConsistentHashableEnvelope(message = msg, hashKey = chatId)
    }
    context.become(awaitResponse(n))
  }

  def awaitResponse(n: Long): Receive = {
    case AskForShards ⇒
      //sender() ! AllShards(shards)
      log.warning("★ ★ ★  stop writer ★ ★ ★")
      context.stop(self)

    case WriteSuccess(_) ⇒
      write(n + 1l)

    /*if (context.system.scheduler ne null)
        context.system.scheduler.scheduleOnce(100.millis, new Runnable {
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