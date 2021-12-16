# MIT 6.824 HWs
## HW1 - MapReduce
Assignment website: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
Implementing a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers.

**To-do:**

**1. worker.go:** 
Implement the worker process. Need to write the main worker function and rpc calls. 
The main worker function mainly consist of three parts:
  - Communicate with the Coordinator to apply for a task via rpc call
    - Thus you need a rpc call function "ApplyForTask"     
  - Do the MAP or REDUCE according to the task assigned by the Coordinator (rpc reply)
  - Report to the Coordinator that the task is finished, and get a reply on whether the task still belongs to itself
    - The task might no longer belongs to itself if it haven't finished for 10 seconds (from the coordinator's point of view, that worker has gone offline)

**2. coordinator.go:**
We need more things for the coordinator. 
  - Coordinator class:
    First we need a coordinator class to store some needed data. We're storing:
    - The current stage we're in - {"map", "reduce", "end"}
    - The number of map tasks and reduce tasks
    - A map (int for the keys) to store remaining tasks
    - A channel to store currently available tasks (available means not haven't finished and haven't been assigned to a worker)
      (The advantage of using a channel is that it will block if it is empty, so if there are no available tasks, the worker will wait until it is non-empty and then gets the RPC reply)
    During the initialization, we put all MAP tasks into remainging tasks, and also put them all into available tasks.
  - Two functions that workers will call via RPC:
    - AssignTask: from all available tasks (in the channel), assign one to the worker who ask for a task. Record which worker this task belongs to.
    - GetReport: get the signal from a worker that it has finished the assigned task, and mark corresponding task stored in the Coordinator class as finished. Reply to the worker on whether the task still belongs to it. 
  - ChangeStage function: When all the MAP or REDUCE tasks are done, we need to switch to the next stage. For MAP -> REDUCE, we need to put all reduce tasks into remaining tasks and available tasks. 

**3. rpc.go:**
Defines the structure of args and reply for rpc call functions.
