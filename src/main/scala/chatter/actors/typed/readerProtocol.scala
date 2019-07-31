package chatter.actors.typed

import chatter.Shard
import akka.stream.SourceRef

import chatter.crdt.ChatTimeline
//import chatter.actors.typed.Replicator.v1.MessagePB

sealed trait ReadReply

case class KnownShards(shards: Vector[Shard[ReplicatorProtocol]]) extends ReadReply

case class RSuccess(tl: ChatTimeline) extends ReadReply
//case class RStreamingSuccess(seq: Seq[MessagePB]) extends ReadReply

case class RFailure(chatName: String) extends ReadReply

case class RNotFound(chatName: String) extends ReadReply

case class RemoteChatTimelineSource(src: SourceRef[Array[Byte]]) extends ReadReply
