package org.screp
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.gracefulStop
import org.screp.actors.MasterActor
import org.screp.fastqutil.{FastqFile, FastqRecord}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.matching.Regex

case class Work(fastqRec: fastqutil.FastqRecord)
case class MapData(matchedFastqRec: Option[fastqutil.FastqRecord])
case class ReduceData(matchedFastqRecList: List[fastqutil.FastqRecord])
case class EndOfExec()
case class EndOfRecord()

object ScrepApplication {
  val usage = "Usage: screp [Regex] [num threads] <FASTQ filename>"
  def main(args: Array[String]) {
    val regex: Regex = args(0).r
    val numThreads: Int = args(1).toInt
    val filename: String = args(2)

    val _system = ActorSystem("screp")

    val fastqRecordIter: Iterator[FastqRecord] = FastqFile(filename).fastqRecordIter

    val master: ActorRef = _system.actorOf(MasterActor.props(regex, numThreads), name = "master")

    fastqRecordIter.foreach((fastqRecord: FastqRecord) => master ! fastqRecord)

    val timeout = Duration(21474835, "seconds")

    try {
      val stopped: Future[Boolean] = gracefulStop(master, timeout, EndOfRecord())
      Await.result(stopped, timeout)
    } catch {
      case e: akka.pattern.AskTimeoutException =>
    }
    _system.terminate()
    // println("Scala done!")
  }
}