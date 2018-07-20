package org.screp.actors

import akka.actor.{Actor, ActorRef, Props}
import org.screp.fastqutil.FastqRecord
import org.screp.MapData
import org.screp.EndOfExec
/**
  * Created by zhangy4 on 1/12/17.
  */
class ReduceActor(val numMapper: Int, val master: ActorRef) extends Actor {
  var finalReducedList : List[FastqRecord] = List()
  var numEndedMapper: Int = 0

  def receive : Receive = {
    case message: MapData => message.matchedFastqRec match {
      case Some(fastqRec: FastqRecord) => {
        finalReducedList = fastqRec :: finalReducedList
        println(fastqRec)
      }
      case None =>
    }
    case eoe: EndOfExec => {
      numEndedMapper += 1
      if (numEndedMapper == numMapper) {
        master ! EndOfExec()
        // println("Reducer: Ended")
        context stop self
      }
    }
  }

}

object ReduceActor {
  def props(numMapper: Int, master: ActorRef): Props =
    Props(new ReduceActor(numMapper, master))
}

