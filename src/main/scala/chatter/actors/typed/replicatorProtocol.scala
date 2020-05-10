package chatter.actors.typed

import akka.actor.typed.ActorRef
import akka.stream.SinkRef
import chatter.crdt.ChatTimeline

sealed trait ReplicatorProtocol

sealed trait ReplicatorWrite      extends ReplicatorProtocol
sealed trait ReplicatorWriteReply extends ReplicatorProtocol

sealed trait ReplicatorRead      extends ReplicatorProtocol
sealed trait ReplicatorReadReply extends ReplicatorProtocol

case class WriteMessage(
  chatId: Long,
  when: Long,
  tz: String,
  userId: Long,
  content: String,
  replyTo: ActorRef[WriteResponses]
) extends ReplicatorWrite

//internal replicator messages protocol
case class RWriteSuccess(chatName: String, replyTo: ActorRef[WriteResponses], start: Long) extends ReplicatorWriteReply
case class RWriteFailure(chatName: String, errorMsg: String, replyTo: ActorRef[WriteResponses])
    extends ReplicatorWriteReply
case class RWriteTimeout(chatName: String, replyTo: ActorRef[WriteResponses]) extends ReplicatorWriteReply

case class ReadChatTimeline(chatId: Long, replyTo: ActorRef[ReadReply]) extends ReplicatorRead

case class PassiveReadChatTimeline(chatId: Long, sinkRef: SinkRef[Array[Byte]], replyTo: ActorRef[ReadReply])
    extends ReplicatorRead

//internal replicator messages protocol
case class RChatTimelineReply(tl: ChatTimeline, replyTo: ActorRef[ReadReply], start: Long) extends ReplicatorReadReply

case class RChatTimelineReplySink(
  tl: ChatTimeline,
  sinkRef: SinkRef[Array[Byte]],
  replyTo: ActorRef[ReadReply],
  start: Long
) extends ReplicatorReadReply

case class RNotFoundChatTimelineReply(chatName: String, replyTo: ActorRef[ReadReply]) extends ReplicatorReadReply
case class RGetFailureChatTimelineReply(error: String, replyTo: ActorRef[ReadReply])  extends ReplicatorReadReply
