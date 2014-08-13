package com.umatrangolo.akka.bb

import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox, Scheduler, ActorLogging, PoisonPill }
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.routing.RoundRobinPool
import akka.util.Timeout

import com.typesafe.config._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

case class Compute(n: Long)
case class Fibonacci(n: Long)

final class Worker extends Actor with ActorLogging {
  import Fibonacci._

  override def receive = {
    case Compute(n: Long) => {
      val now = System.currentTimeMillis
      val fib = fibonacciEventually(n)
      log.info(s"Fibonacci($n) = $fib [" + (System.currentTimeMillis - now) + s" ms.]")
      sender ! fib
    }
    case msg => throw new UnsupportedOperationException(s"Unsupported message $msg")
  }

  override def preStart() { log.info(self + " is starting ...") }
  override def preRestart(reason: Throwable, msg: Option[Any]) { log.info(self + " is restarting ...") }
  override def postStop() { log.info(self + " has been stopped") }
  override def postRestart(reason: Throwable) { log.info(self + " has been restarted") }
}

object Fibonacci {
  import Rnds._

  def fibonacci(n: Long): Long = n match {
    case 0 => 1
    case 1 => 1
    case n => fibonacci(n - 2) + fibonacci(n - 1)
  }

  def fibonacciEventually(n: Long): Long = if (rnd(10) < 1) throw new NullPointerException("Ouch!") else fibonacci(n)
}

object Rnds {
  def rnd(n: Int) = scala.util.Random.nextInt(n)
  def rndInput = rnd(50)
  def rndWait = rnd(500)
}

object MasterWorkersApp extends App {
  import Rnds._

  println("Starting Fibonacci Master-Worker ...")

  val system = ActorSystem("master-worker")

  val escalator = OneForOneStrategy() {
    case _: Exception => Restart
  }

  // this will create a Router actor that will then create Worker routees as its child
  val master = system.actorOf(RoundRobinPool(
    nrOfInstances = 5,
    supervisorStrategy = escalator
  ).props(routeeProps = Props[Worker]), "master")

  implicit val timeout = Timeout(5 seconds)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() {
      implicit val inbox = Inbox.create(system)
      println("Shutting down....")
      master ! PoisonPill
      println("Bye.")
    }
  })

  for (i <- 1 to 1000) {
    ask(master, Compute(rndInput))
    Thread.sleep(rndWait)
  }

}
