package chatter
package actors

import ChatTimelineReader._
import scala.concurrent.duration._
import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import chatter.actors.ChatTimelineWriter.{ ReadLocalChatTimeline, ReadRemoteChatTimeline }
import chatter.actors.ChatTimelineReader.{ AllShards, AskForShards, LocalChatTimelineResponse, NotFoundChatTimelineResponse, RemoteChatTimelineResponse }

object ChatTimelineReader {

  case object AskForShards

  case class AllShards(shards: Vector[Shard])

  sealed trait ReadResponses

  case class LocalChatTimelineResponse(history: Vector[Message]) extends ReadResponses

  case class RemoteChatTimelineResponse(history: Vector[Message]) extends ReadResponses

  case class NotFoundChatTimelineResponse(chatName: String) extends ReadResponses

  case class GetFailureChatTimelineResponse(chatName: String) extends ReadResponses

  def isSorted(in: Vector[Message]): Boolean =
    (1 until in.length).forall(i ⇒ in(i - 1).when <= in(i).when)

  def props(writer: ActorRef) =
    Props(new ChatTimelineReader(writer))
}

class ChatTimelineReader(writer: ActorRef) extends Actor with ActorLogging {

  val readTO = 50.millis
  implicit val ex = context.dispatcher

  writer ! AskForShards

  override def receive: Receive = {
    case AllShards(shards) ⇒
      log.info("shards: {}", shards.toString)
      read(0l, shards)
  }

  def read(chatId: Long, shards: Vector[Shard]): Unit = {
    val ind = (chatId % shards.size).toInt
    log.info("read chat-{} -> ind:{}", chatId, ind)
    shards(ind) match {
      case LocalShard(_, ref) ⇒
        ref ! ReadLocalChatTimeline(s"chat-$chatId")
      case RemoteShard(_, ref) ⇒
        val msg = ReadRemoteChatTimeline(s"chat-$chatId")
        ref ! ConsistentHashableEnvelope(message = msg, hashKey = chatId)
    }
    context.become(await(chatId, shards))
  }

  def await(chatId: Long, shards: Vector[Shard]): Receive = {
    case LocalChatTimelineResponse(h) ⇒
      log.warning("chat-{} = {}", chatId, h.size)
      context.system.scheduler.scheduleOnce(readTO, new Runnable {
        override def run = read(chatId + 1l, shards)
      })
      context.become(await(chatId, shards))

    case RemoteChatTimelineResponse(h) ⇒
      log.info("chat-{} = {}", chatId, h.size)
      context.system.scheduler.scheduleOnce(readTO, new Runnable {
        override def run = read(chatId + 1l, shards)
      })
      context.become(await(chatId, shards))

    case GetFailureChatTimelineResponse(ex) ⇒
      log.error(ex, "GetFailureChat")
      context.stop(self)

    case NotFoundChatTimelineResponse(name) ⇒
      log.warning("NotFoundChatTime: " + name)
      context.become(done)
  }

  def done: Receive = {
    case _ ⇒
      log.warning("reader is done")
  }
}
