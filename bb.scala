import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox, Scheduler }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case object TryProduce
case object TryConsume
case object Get
case class Put(v: Int)
case class Success(v: Int)
case object Full
case object Empty
case object WakeUp

final class BoundedBuffer(capacity: Int) extends Actor {
  import Utils._

  implicit val color = Console.GREEN
  private val who = s"Buffer($capacity)"

  say(who, s"Starting Bounded Buffer with capacity: $capacity")

  private var count = 0
  private val maxCapacity = capacity
  private val buffer = new Array[Int](capacity)

  private val waitingProducers = scala.collection.mutable.ListBuffer.empty[ActorRef]
  private val waitingConsumers = scala.collection.mutable.ListBuffer.empty[ActorRef]

  def receive = {
    case Put(v) if (!amIFull) => {
      store(v)
      say(who, s"Stored value: $v, count: $count")
      awakeConsumers
      sender ! Success(v)
    }
    case Put(v) if (amIFull) => {
      say(who, s"Full buffer for value: $v, count: $count")
      waitingProducers += sender
      sender ! Full
    }
    case Get if (!amIEmpty) => {
      val value = fetch()
      say(who, s"Fetched value: $value, count: $count")
      awakeProducers
      sender ! Success(value)
    }
    case Get if (amIEmpty) => {
      say(who, s"Empty buffer, count: $count")
      waitingConsumers += sender
      sender ! Empty
    }
    case msg => say(who, s"Unrecognized message: $msg")
  }

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

final class Producer(id: Int, buffer: ActorRef, scheduler: Scheduler) extends Actor {
  import Utils._

  implicit val color = Console.RED
  private val who = s"Producer-$id"

  private[this] var sleeping = false
  private[this] var waiting = false

  say(who, s"Starting Producer with id: $id, buffer is $buffer")

  override def receive = {
    case TryProduce if (!waiting && !sleeping) => {
      val value = produce()
      say(who, s"Trying to produce value: $value ...")
      waiting = true
      buffer ! Put(value)
    }
    case Full if (waiting) => {
      say(who, "Got a Full from the Buffer. Going to sleep...")
      waiting = false
      sleeping = true
    }
    case Success(v) if (waiting) => {
      say(who, s"Successfully produced value: $v")
      waiting = false
      scheduler.scheduleOnce(scala.math.abs(scala.util.Random.nextInt(500)) milliseconds) {
        self ! TryProduce
      }
    }
    case WakeUp if (sleeping) => {
      say(who, "Got a WakeUp from Buffer. Trying to produce ...")
      sleeping = false
      self ! TryProduce
    }
    case msg => say(who, s"Got $msg while sleeping: $sleeping or waiting: $waiting. Ignoring...", true)
  }

  private def produce(): Int = scala.math.abs(scala.util.Random.nextInt(1000))
}

final class Consumer(id: Int, buffer: ActorRef, scheduler: Scheduler) extends Actor {
  import Utils._

  private[this] var sleeping = false
  private[this] var waiting = false

  implicit val color = Console.YELLOW
  private val who = s"Consumer-$id"
  say(who, s"Starting Consumer with id: $id, buffer is $buffer")

  override def receive = {
    case TryConsume if (!waiting && !sleeping) => {
      say(who, "Trying to consume ...")
      buffer ! Get
      waiting = true
    }
    case Empty if (waiting) => {
      say(who, "Buffer is empty. Going to sleep ...")
      waiting = false
      sleeping = true
    }
    case Success(v) if (waiting) => {
      say(who, s"Successfully consumed value: $v")
      waiting = false
      scheduler.scheduleOnce(scala.math.abs(scala.util.Random.nextInt(500)) milliseconds) {
        self ! TryConsume
      }
    }
    case WakeUp if (sleeping) => {
      say(who, "Got a WakeUp from Buffer. Trying to consume ...")
      sleeping = false
      self ! TryConsume
    }
    case msg => say(who, s"Got $msg while sleeping: $sleeping or waiting: $waiting. Ignoring...", true)
  }
}

object Utils {
  def say(who: String, msg: String, error: Boolean = false)(implicit color: String) {
    println(color + new java.util.Date + " " + who + { if (error) " [ERROR]" } + s" $msg")
  }
}


object BB extends App {
  val producers = 2
  val consumers = 4
  val capacity = 10
  println(s"Starting Bounded Buffer (producers: $producers, consumers: $consumers, capacity: $capacity)")

  val system = ActorSystem("bounded-buffer") // Create the 'bounded-buffer' system
  val scheduler = system.scheduler
  val inbox = Inbox.create(system) // Create an "actor-in-a-box"

  val boundedBuffer = system.actorOf(Props(classOf[BoundedBuffer], capacity), "bounded-buffer") // Create the 'bounded-buffer' actor

  for (i <- 1 to producers) {
    val p = system.actorOf(Props(classOf[Producer], i, boundedBuffer, scheduler), "producer-" + i)
    inbox.send(p, TryProduce)
  }
  for (i <- 1 to consumers) {
    val c = system.actorOf(Props(classOf[Consumer], i, boundedBuffer, scheduler), "consumer-" + i)
    inbox.send(c, TryConsume)
  }
}
