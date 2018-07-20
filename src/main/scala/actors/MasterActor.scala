package org.screp.actors

import akka.routing.{Broadcast, RoundRobinPool}
import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import org.screp.{EndOfExec, EndOfRecord}
import org.screp.fastqutil.FastqRecord

import scala.util.matching.Regex

/**
  * Created by zhangy4 on 1/12/17.
  */
class MasterActor(regex: Regex, numMapper: Int) extends Actor {
  private val reducer: ActorRef =
    context.actorOf(
      ReduceActor.props(numMapper, self),
      name = "reducer")
  private val router: ActorRef =
    context.actorOf(
      RoundRobinPool(numMapper).props(MapActor.props(regex, reducer)),
      "router")

  def receive: Receive = {
    case message: FastqRecord => {
      // println("master: test");
      router ! message
    }
    case _: EndOfRecord => {
      // println("master: eor");
      router.tell(new Broadcast(EndOfRecord()), self)
    }
    case _: EndOfExec => context stop self
  }

}

object MasterActor {
  def props(regex: Regex, numMapper: Int): Props =
    Props(new MasterActor(regex, numMapper))
}

