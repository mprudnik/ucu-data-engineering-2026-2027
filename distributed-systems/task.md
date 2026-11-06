We are going to work on my ucu course tasks, program is called data engineering and course is distributed systems.
You can probably get some context from other chats that we had (if you can do so).
Overall there are 3 task iterations and all about replicated log, each iteration differ in requirements and assumptions.
Common requirements:
- 3 nodes
- 1 master 2 replicas
- simple communication using HTTP (can be something other)
- master has append and list commands, replicas have list command only
- docker - should be able to spin up everything
- tests - we should have test harness that will allow us to test all cases, it should avoid real network
- docs - in code docs + readme to explain how to use it
Per iteration:
iter-1:
- basic commands for master, no options passed
- ideal channel, no message is lost
- sync replication - no reponse until replicas ack written message (not sure if correct to call it sync, maybe its write-concern=all, or maybe it's the same)

iter-2:
- starting with same as iter-1
- commands have write concern (1, 2, 3)
- replication is async
- tests should simulate delays to mimic real world and see write concerns in practice

iter-3:
- starting with same as iter-2
- simulate message drops
- implement retry
- implement deduplication - since we have retry
- implement heartbeats

test harness main tasks:
- message delays
- message drops
- message reordering

For test harness to allow it, we need properly separate source code to allow simple in-memory test harness (keep data in memory, e.g. disk emulated as vectors, networks emulated as queues, but everything in memory, so we'll be able to "tell" harness, "delay message X", "reorder messages X,Y", etc). We should be able to inject harness instead of communication channel and simulate all cases, in both request and response messages.

You won't write any code, only guide and check me. (maybe with some small exceptions in tests and comments/docs)

Original document with all 3 tasks and harness requirements here - @../Replicated log task.pdf .
