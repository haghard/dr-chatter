package chatter.actors.typed

import akka.actor.typed.ActorRef
import chatter.{ Message, Shard }

sealed trait WriteResponses

case object StartWriting extends WriteResponses

case class AskForShards(replyTo: ActorRef[ReadReply]) extends WriteResponses

case class WSuccess(chatName: String) extends WriteResponses

case class WFailure(chatName: String, errorMsg: String) extends WriteResponses

case class WTimeout(chatName: String) extends WriteResponses

sealed trait ReadReply

case class KnownShards(shards: Vector[Shard[ReplicatorCommand]]) extends ReadReply

case class RSuccess(history: Vector[Message]) extends ReadReply

case class RFailure(chatName: String) extends ReadReply

case class RNotFound(chatName: String) extends ReadReply

trait ReplicatorCommand

case class WriteMessage(chatName: String, when: Long, tz: String, authId: Long, content: String, replyTo: ActorRef[WriteResponses]) extends ReplicatorCommand

case class ReadChatTimeline(chatName: String, replyTo: ActorRef[ReadReply]) extends ReplicatorCommand