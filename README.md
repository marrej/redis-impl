# Redis Impl

This project has served me as learning experience for C#.
With that said, I tackled few interesting challenges, such as multithreading for active storage, creating layers over existing thread contention mechanisms and more.

Note that it doesn't implement any security features so its not possible to use in prod env, onyl as toy project locally

## Usage

Start via `dotnet watch` as master or e.g. `dotnet watch --port 1234 --replicaof "localhost 6379"` as replica connected to master.

Easiest way how to interact with the app is `redis-cli` which automatically opens a TCP socket to communicate with the service. To communicate e.g. with replicas choose a different port `redis-cli -p 1234`.

## General workflow

`Server.CS` handles all incomming traffic. After a connection is made, it uses `Parser` to parse the input and `Interpreter` to launch the command.

A shared `Storage` is used for all the sockets. The storage is a single class which should be split for Streams,Arrays,KeyVals but currently is a single storage.

`MasterReplicaBridge` is used to coordinate the replication handshake and consumation of queued commands from Master.

## Learnings

- Semaphores work generally better than Mutex if you want to guard a resource, since they can be released multiple times without crashing and shouldn't cause deadlocks if combined with Timers.
- In a system like this, it would be beneficial to have a class executor per command, with which some complexity over the Storage usage would dissapear (assuming that properties from storage would be public)
- TCP communication is best leveraged with REQ-RES model. Although that consumes more bandwidth, its less error prone. Adding the selective REQ/RES and bundling messages together can cause a lot of pain to understand analyse and also trigger unwanted receivals. (e.g. message duplication)

## Appendix
This was largely implemented via the course - https://app.codecrafters.io/courses/redis/introduction , but worth to note that I was adding a bit more on top since having unfinished features frustrated me.
The course is not 100% finished due to raciness in the evaluator when multiple replicas were connected.