import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox, Scheduler}
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case object Produce
case object Consume
case object Get
case class Put(v: Int)
case class Success(v: Int)
case object Full
case object Empty
case object WakeUp

final class BoundedBuffer(capacity: Int) extends Actor {
  import Utils._
  import context._

  implicit val color = Console.GREEN
  private val who = s"Buffer($capacity)"

  say(who, s"Starting Bounded Buffer with capacity: $capacity")

  private var count = 0
  private val maxCapacity = capacity
  private val buffer = new Array[Int](capacity)

  private val waitingProducers = scala.collection.mutable.ListBuffer.empty[ActorRef]
  private val waitingConsumers = scala.collection.mutable.ListBuffer.empty[ActorRef]

  def available: Receive = {
    case Get => {
      val value = fetch()
      say(who, s"Fetched value: $value, count: $count")
      if (amIEmpty) {
        say(who, "Switching to EMPTY state")
        become(empty)
      }
      sender ! Success(value)
    }
    case Put(v) => {
      store(v)
      say(who, s"Stored value: $v, count: $count")
      if (amIFull) {
        say(who, "Switching to FULL state")
        become(full)
      }
      sender ! Success(v)
    }
    case msg => say(who, s"Incorrect message for my status (AVAILABLE): $msg", true)
  }

  def full: Receive = {
    case Put(v) => {
      say(who, s"Full buffer for value: $v, count: $count")
      waitingProducers += sender
      sender ! Full
    }
    case Get => {
      val value = fetch()
      say(who, s"Fetched value: $value, count: $count")
      awakeProducers()
      become(available)
      sender ! Success(value)
    }
    case msg => say(who, s"Incorrect message for my status (FULL): $msg", true)
  }

  def empty: Receive = {
    case Get => {
      say(who, s"Empty buffer, count: $count")
      waitingConsumers += sender
      sender ! Empty
    }
    case Put(v) => {
      store(v)
      say(who, s"Stored value: $v, count: $count")
      awakeConsumers()
      become(available)
      sender ! Success(v)
    }
    case msg => say(who, s"Incorrect message for my status (EMPTY): $msg", true)
  }

  override def receive = empty   // initially the buffer is empty

  private def store(v: Int) {
    buffer(count) = v
    count = count + 1
  }

  private def fetch(): Int = {
    count = count - 1
    val v = buffer(count)
    v
  }

  private def awakeProducers() {
    if (!waitingProducers.isEmpty) {
      val toWakeUp = waitingProducers.clone
      waitingProducers.clear
      toWakeUp.foreach { p =>
        say(who, s"Awaking producer: $p")
        p ! WakeUp
      }
    }
  }

  private def awakeConsumers() {
    if (!waitingConsumers.isEmpty) {
      val toWakeUp = waitingConsumers.clone
      waitingConsumers.clear
      toWakeUp.foreach { c =>
        say(who, s"Awaking consumer: $c")
        c ! WakeUp
      }
    }
  }

  private def amIFull() = count == maxCapacity
  private def amIEmpty() = count == 0
  private def amIAvailable() = !amIFull() && !amIEmpty()
}

final class ProducerSupervisor extends Actor {
  import Utils._
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import scala.concurrent.duration._

  implicit val color = Console.BLACK
  private val who = s"ProducerSupervisor"

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: Exception                => Restart
    }

  override def receive = {
    case p: Props => sender ! context.actorOf(p)
    case msg => say(who, s"Unrecognized message. $msg", true)
  }
}

class Producer(id: Int, buffer: ActorRef, scheduler: Scheduler) extends Actor {
  import Utils._
  import context._

  implicit val color = Console.RED
  private val who = s"Producer-$id"

  say(who, s"Starting Producer with id: $id, buffer is $buffer")

  override def postRestart(reason: Throwable) = {
    self ! Produce
  }

  def producing: Receive = {
    case Produce => {
      dieEventually()
      val value = produce()
      say(who, s"Trying to produce value: $value ...")
      become(waiting)
      buffer ! Put(value)
    }
    case msg => say(who, s"Incorrect message for my status (PRODUCING): $msg", true)
  }

  def waiting: Receive = {
    case Success(v) => {
      dieEventually()
      say(who, s"Successfully produced value: $v")
      become(producing)
      scheduler.scheduleOnce(scala.math.abs(scala.util.Random.nextInt(500)) milliseconds) {
        self ! Produce
      }(scala.concurrent.ExecutionContext.Implicits.global)
    }
    case Full => {
      dieEventually()
      say(who, "Got a Full from the Buffer. Going to sleep...")
      become(sleeping)
    }
    case msg => say(who, s"Incorrect message for my status (WAITING): $msg", true)
  }

  def sleeping: Receive = {
    case WakeUp => {
      dieEventually()
      say(who, "Got a WakeUp from Buffer. Trying to produce ...")
      become(producing)
      self ! Produce
    }
    case msg => say(who, s"Incorrect message for my status (SLEEPING): $msg", true)
  }

  override def receive = producing

  private def produce(): Int = scala.math.abs(scala.util.Random.nextInt(1000))
}

final class ConsumerSupervisor extends Actor {
  import Utils._
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import scala.concurrent.duration._

  implicit val color = Console.BLACK
  private val who = s"ConsumerSupervisor"

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
      case _: Exception                => Restart
    }

  override def receive = {
    case c: Props => sender ! context.actorOf(c)
    case msg => say(who, s"Unrecognized message. $msg", true)
  }
}

final class Consumer(id: Int, buffer: ActorRef, scheduler: Scheduler) extends Actor {
  import Utils._
  import context._

  implicit val color = Console.YELLOW
  private val who = s"Consumer-$id"
  say(who, s"Starting Consumer with id: $id, buffer is $buffer")

  override def postRestart(reason: Throwable) = {
    self ! Consume
  }

  def consuming: Receive = {
    case Consume => {
      dieEventually()
      say(who, "Trying to consume ...")
      buffer ! Get
      become(waiting)
    }
    case msg => say(who, s"Incorrect message for my status (CONSUMING): $msg", true)
  }

  def waiting: Receive = {
    case Success(v) => {
      dieEventually()
      say(who, s"Successfully consumed value: $v")
      become(consuming)
      scheduler.scheduleOnce(scala.math.abs(scala.util.Random.nextInt(500)) milliseconds) {
        self ! Consume
      }(scala.concurrent.ExecutionContext.Implicits.global)
    }
    case Empty => {
      dieEventually()
      say(who, "Buffer is empty. Going to sleep ...")
      become(sleeping)
    }
    case msg => say(who, s"Incorrect message for my status (WAITING): $msg", true)
  }

  def sleeping: Receive = {
    case WakeUp => {
      dieEventually()
      say(who, "Got a WakeUp from Buffer. Trying to consume ...")
      become(consuming)
      self ! Consume
    }
    case msg => say(who, s"Incorrect message for my status (SLEEPING): $msg", true)
  }

  override def receive = consuming
}

object Utils {
  def say(who: String, msg: String, error: Boolean = false)(implicit color: String) {
    println(color + new java.util.Date + " " + who + { if (error) " [ERROR]" } + s" $msg")
  }

  def dieEventually() {
    if (scala.math.abs(scala.util.Random.nextInt(100)) <= 10) throw new NullPointerException("Ouch!")
  }
}

object BB extends App {
  val producers = 1
  val consumers = 1
  val capacity = 2
  println(s"Starting Bounded Buffer (producers: $producers, consumers: $consumers, capacity: $capacity)")

  val system = ActorSystem("bounded-buffer") // Create the 'bounded-buffer' system
  val scheduler = system.scheduler
  implicit val inbox = Inbox.create(system) // Create an "actor-in-a-box"

  val boundedBuffer = system.actorOf(Props(classOf[BoundedBuffer], capacity), "bounded-buffer") // Create the 'bounded-buffer' actor
  val producerSupervisor = system.actorOf(Props[ProducerSupervisor], "producer-supervisor")
  val consumerSupervisor = system.actorOf(Props[ConsumerSupervisor], "consumer-supervisor")

  implicit val timeout = Timeout(5 seconds)

  for (i <- 1 to producers) {
    ask(producerSupervisor, Props(classOf[Producer], i, boundedBuffer, scheduler)).mapTo[ActorRef].map { _ ! Produce }
  }

  for (i <- 1 to producers) {
    ask(consumerSupervisor, Props(classOf[Consumer], i, boundedBuffer, scheduler)).mapTo[ActorRef].map { _ ! Consume }
  }
}
