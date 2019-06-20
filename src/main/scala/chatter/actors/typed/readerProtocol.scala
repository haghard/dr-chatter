package chatter.actors.typed

import chatter.crdt.ChatTimeline
import chatter.Shard

sealed trait ReadReply

case class KnownShards(shards: Vector[Shard[ReplicatorProtocol]]) extends ReadReply

case class RSuccess(tl: ChatTimeline) extends ReadReply

case class RFailure(chatName: String) extends ReadReply

case class RNotFound(chatName: String) extends ReadReply
