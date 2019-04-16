package chatter.actors.typed

import chatter.{ Message, Shard }

sealed trait ReadReply

case class KnownShards(shards: Vector[Shard[ReplicatorCommand]]) extends ReadReply

case class RSuccess(history: Vector[Message]) extends ReadReply

case class RFailure(chatName: String) extends ReadReply

case class RNotFound(chatName: String) extends ReadReply
