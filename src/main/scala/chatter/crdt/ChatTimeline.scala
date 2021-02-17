package chatter
package crdt

import scala.collection.Searching._

/*
TODO: Rather than having to replicate the entirety of the ChatTimeline every time,
it would be much better to only synchronize those bits that have indeed be subjected to a change.
This can be achieved by implementing the akka.cluster.ddata.DeltaReplicatedData
 */
case class ChatTimeline(
  timeline: Vector[Message] = Vector.empty[Message],
  versions: VersionVector[Node] = VersionVector.empty[Node](Implicits.nodeOrd)
) extends akka.cluster.ddata.ReplicatedData {

  override type T = ChatTimeline

  def +(msg: Message, node: Node): ChatTimeline = {
    val ip: SearchResult = timeline.search(msg)(Implicits.msgOrd)
    if (ip.insertionPoint == 0)
      copy(msg +: timeline, versions + node)
    else if (ip.insertionPoint == timeline.size)
      copy(timeline :+ msg, versions + node)
    else {
      val (a, b) = timeline.splitAt(ip.insertionPoint)
      copy(a ++ (msg +: b), versions + node)
    }
  }

  //Sort 2 sorted arrays saving messages with the same ts, never drop messages
  private def merge0(tlA: Vector[Message], tlB: Vector[Message]): Vector[Message] = {
    @scala.annotation.tailrec
    def divergedIndex(a: Vector[Message], b: Vector[Message], limit: Int, i: Int = 0): Option[Int] =
      if (i < limit)
        if (a(i) != b(i)) Some(i) else divergedIndex(a, b, limit, i + 1)
      else None

    divergedIndex(tlA, tlB, math.min(tlA.length, tlB.length)) match {
      case Some(i) ⇒
        val (same, a)   = tlA.splitAt(i)
        val (_, b)      = tlB.splitAt(i)
        var iA          = a.length - 1
        var iB          = b.length - 1
        var mergeResult = Vector.fill[Message](a.length + b.length)(null)
        var limit       = mergeResult.length
        while (limit > 0) {
          limit -= 1
          val elem = if (iB < 0 || (iA >= 0 && a(iA).when >= b(iB).when)) {
            iA -= 1
            a(iA + 1)
          } else {
            iB -= 1
            b(iB + 1)
          }
          mergeResult = mergeResult.updated(limit, elem)
        }
        same ++ mergeResult
      case None ⇒
        if (tlA.size > tlB.size) tlA else tlB
    }
  }

  /*
    Requires a bounded semilattice (or idempotent commutative monoid).
    Monotonic semi-lattice + merge = Least Upper Bound

    We rely on commutivity to ensure that machine A merging with machine B yields the same result as machine B merging with machine A.
    We need associativity to ensure we obtain the correct result when three or more machines are merging data.
    We need an identity element to initialise empty timeline.
    Finally, we need idempotency, to ensure that if two machines hold the same data
    in a per-machine ChatTimeline, merging them will not lead to an incorrect result.
   */
  override def merge(that: ChatTimeline): ChatTimeline =
    //that dominates this
    if (versions < that.versions)
      that
    else //this dominates that
    if (versions > that.versions)
      this
    else //concurrent
    if (versions <> that.versions) {
      val r = merge0(timeline, that.timeline)
      /*val s = System.currentTimeMillis
      val l = System.currentTimeMillis - s
      println(s"${versions.elems.map { case (n, v) ⇒ s"${n.port}:$v" }.mkString(",")} vs ${that.versions.elems
        .map { case (n, v)                         ⇒ s"${n.port}:$v" }
        .mkString(",")} latency:$l")*/
      ChatTimeline(r, versions merge that.versions)
    } else this //means ==
}
