package chatter.actors.typed

import akka.actor.typed.ActorRef

sealed trait WriteResponses

case object StartWriting extends WriteResponses

case class AskForShards(replyTo: ActorRef[ReadReply]) extends WriteResponses

case class WSuccess(chatName: String) extends WriteResponses

case class WFailure(chatName: String, errorMsg: String) extends WriteResponses

case class WTimeout(chatName: String) extends WriteResponses