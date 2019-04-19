package chatter.actors.typed

import akka.actor.typed.ActorRef
import chatter.crdt.ChatTimeline

trait ReplicatorCommand

case class WriteMessage(
    chatId: Long,
    when: Long, tz: String, authId: Long, content: String, replyTo: ActorRef[WriteResponses]) extends ReplicatorCommand

case class ReadChatTimeline(chatId: Long, replyTo: ActorRef[ReadReply]) extends ReplicatorCommand

//internal replicator messages protocol
case class RWriteSuccess(chatName: String, replyTo: ActorRef[WriteResponses]) extends ReplicatorCommand

case class RWriteFailure(chatName: String, errorMsg: String, replyTo: ActorRef[WriteResponses]) extends ReplicatorCommand

case class RWriteTimeout(chatName: String, replyTo: ActorRef[WriteResponses]) extends ReplicatorCommand

case class RChatTimelineReply(tl: ChatTimeline, replyTo: ActorRef[ReadReply]) extends ReplicatorCommand

case class RNotFoundChatTimelineReply(chatName: String, replyTo: ActorRef[ReadReply]) extends ReplicatorCommand

case class RGetFailureChatTimelineReply(error: String, replyTo: ActorRef[ReadReply]) extends ReplicatorCommand