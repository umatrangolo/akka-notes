# Bounded Buffer using Akka

Various iterations of a Bounded Buffer solution using an incrementally
more elegant Akka solution.

* 1bf5f0e - (HEAD, origin/master, master) Adding proper shutdown with PoisonPill
* c8ad2b3 - Configuring Dispatchers and proper logging
* 5d8a332 - Using Logging subsystem from Akka/Slf4j support
* 351ff9d - Rewritten using FSM module
* 32d30b9 - Fault tolerance on failing Producer/Consumer
* 9952f72 - Using become() to switch status
* cc7bb29 - Very naive Bounded Buffer
* d0ebd63 - Initial commit
