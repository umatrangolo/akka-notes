import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox, Scheduler, FSM }
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.LinearSeq

case object Produce
case object Consume

// received events
case object Get
case class Put(v: Int)

// sent events
case object Full
case object Empty
case class Success(v: Int)
case object WakeUp

// states
sealed trait State
case object EmptyBuffer extends State
case object FullBuffer extends State
case object Available extends State

sealed trait Data
case class Buffer(count: Int, bb: LinearSeq[Int], waitingProducers: LinearSeq[ActorRef], waitingConsumers: LinearSeq[ActorRef]) extends Data
case object NoData extends Data

final class BoundedBuffer(val capacity: Int) extends Actor with FSM[State, Data] {
  import Utils._

  implicit val color = Console.GREEN
  private val who = s"Buffer($capacity)"

  say(who, s"Starting Bounded Buffer with capacity: $capacity")

  startWith(EmptyBuffer, Buffer(0, LinearSeq.empty[Int], LinearSeq.empty[ActorRef], LinearSeq.empty[ActorRef]))

  initialize()

  onTransition {
    case EmptyBuffer -> Available => say(who, "EMPTY -> AVAILABLE")
    case Available -> EmptyBuffer => say(who, "AVAILABLE -> EMPTY")
    case Available -> FullBuffer => say(who, "AVAILABLE -> FULL")
    case FullBuffer -> Available => say(who, "FULL -> AVAILABLE")
  }

  when(EmptyBuffer) {
    case Event(Get, buffer: Buffer) => stay using buffer.copy(waitingConsumers = sender +: buffer.waitingConsumers) replying (Empty)
    case Event(Put(v), buffer: Buffer) => {
      awakeConsumers(buffer.waitingConsumers)
      goto(Available) using buffer.copy(1, LinearSeq(v), waitingConsumers = LinearSeq.empty[ActorRef]) replying (Success(v))
    }
  }

  when(FullBuffer) {
    case Event(Put(v), buffer: Buffer) => stay using buffer.copy(waitingProducers = sender +: buffer.waitingProducers) replying (Full)
    case Event(Get, buffer: Buffer) => {
      val v = buffer.bb.head
      awakeProducers(buffer.waitingProducers)
      goto(Available) using buffer.copy(capacity - 1, buffer.bb.tail, waitingProducers = LinearSeq.empty[ActorRef]) replying (Success(v))
    }
  }

  when(Available) {
    case Event(Put(v), buffer: Buffer) if (buffer.count + 1 == capacity) =>
      goto(FullBuffer) using buffer.copy(count = capacity, bb = v +: buffer.bb) replying (Success(v))
    case Event(Put(v), buffer: Buffer) => stay using buffer.copy(count = buffer.count + 1, bb = v +: buffer.bb) replying (Success(v))
    case Event(Get, buffer: Buffer) if (buffer.count - 1 == 0) => {
      val v = buffer.bb.head
      goto(EmptyBuffer) using buffer.copy(count = 0, bb = LinearSeq.empty[Int]) replying (Success(v))
    }
    case Event(Get, buffer: Buffer) => {
      val v = buffer.bb.head
      stay using buffer.copy(count = buffer.count - 1, bb = buffer.bb.tail) replying (Success(v))
    }
  }

  whenUnhandled {
    case Event(msg, data) => {
      say(who, s"Received unhandled event in my current state. msg: $msg, data: $data", true)
      stay
    }
  }

  def awakeProducers(producers: LinearSeq[ActorRef]) {
    producers.foreach { p =>
      say(who, s"Awaking producer: $p")
      p ! WakeUp
    }
  }

  def awakeConsumers(consumers: LinearSeq[ActorRef]) {
    consumers.foreach { c =>
      say(who, s"Awaking consumer: $c")
      c ! WakeUp
    }
  }
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

// state
case object Producing extends State
case object Waiting extends State
case object Sleeping extends State

case class Value(v: Int) extends Data

class Producer(id: Int, buffer: ActorRef, scheduler: Scheduler) extends Actor with FSM[State, Data] {
  import Utils._

  implicit val color = Console.RED
  private val who = s"Producer-$id"

  say(who, s"Starting Producer with id: $id, buffer is $buffer")

  startWith(Producing, Value(produce()))

  initialize()

  override def postRestart(reason: Throwable) = {
    initialize()
    self ! Produce
  }

  when(Producing) {
    case Event(Produce, Value(v)) => {
      buffer ! Put(v)
      goto(Waiting) using Value(v)
    }
  }

  when(Waiting) {
    case Event(Success(v1), Value(v2)) => {
      require(v1 == v2)
      goto(Producing) using Value(produce())
    }
    case Event(Full, Value(v)) => goto(Sleeping) using Value(v)
  }

  when (Sleeping) {
    case Event(WakeUp, Value(v)) => goto(Waiting) using Value(v) replying(Put(v))
  }

  whenUnhandled {
    case Event(msg, data) => {
      say(who, s"Received unhandled event in my current state. msg: $msg, data: $data", true)
      stay
    }
  }

  onTransition {
    case Producing -> Waiting => { say(who, "PRODUCING -> WAITING"); dieEventually() }
    case Waiting -> Sleeping => { say(who, "WAITING -> SLEEPING"); dieEventually() }
    case Sleeping -> Waiting => { say(who, "SLEEPING -> WAITING"); dieEventually() }
    case Waiting -> Producing => {
      say(who, "WAITING -> PRODUCING")
      setTimer("producing", Produce, Duration(scala.math.abs(scala.util.Random.nextInt(1000)), "milliseconds"), false)
      dieEventually()
    }
  }

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

// state
case object Consuming extends State

final class Consumer(id: Int, buffer: ActorRef, scheduler: Scheduler) extends Actor with FSM[State, Data] {
  import Utils._

  implicit val color = Console.YELLOW
  private val who = s"Consumer-$id"
  say(who, s"Starting Consumer with id: $id, buffer is $buffer")

  startWith(Consuming, NoData)

  initialize()

  override def postRestart(reason: Throwable) = {
    initialize()
    self ! Consume
  }

  when(Consuming) {
    case Event(Consume, NoData) => {
      buffer ! Get
      goto(Waiting)
    }
  }

  when(Waiting) {
    case Event(Success(v), NoData) => goto(Consuming)
    case Event(Empty, NoData) => goto(Sleeping)
  }

  when(Sleeping) {
    case Event(WakeUp, NoData) => {
      buffer ! Get
      goto(Waiting)
    }
  }

  whenUnhandled {
    case Event(msg, data) => {
      say(who, s"Received unhandled event in my current state. msg: $msg, data: $data", true)
      stay
    }
  }

  onTransition {
    case Consuming -> Waiting => { say(who, "CONSUMING -> WAITING"); dieEventually() }
    case Waiting -> Consuming => {
      setTimer("consuming", Consume, Duration(scala.math.abs(scala.util.Random.nextInt(1000)), "milliseconds"), false)
      say(who, "WAITING -> CONSUMING")
      dieEventually()
    }
    case Waiting -> Sleeping => { say(who, "WAITING -> SLEEPING"); dieEventually() }
    case Sleeping -> Waiting => { say(who, "SLEEPING -> WAITING"); dieEventually() }
  }
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
  val capacity = 10
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
