# 2PC

**More Technical Explanation of a 2 Phase Commit**:

You have a distributed system of servers (e.g. server1 - server5)
- Coordinator: In charge of modifying the global state
- Participants: Only modify the local state

Each node is inactive until a client makes a request. When the client sends the request, the system will randomly designate a coordinator (with the rest being participants).

**Probing Phase**: Coordinator sends a command of what the client wants it to do to the participants
- Each participant independently executes the operation; depending on whether they were successful, they send a commit/abort vote to the coordinator
- Coordinator tallies up the votes; if all of them are commit votes, then it commits; otherwise, it aborts
  
**Commit Phase**: Commit happens or doesn’t (depending on the tally)
- Coordinator sends a message back to the participants (if there was an abort, participants that sent a commit vote must rollback that operation)

The code I wrote simulates how many commits/aborts were done given a certain system configuration (You can tune the success rate of the local participants, coordinator, number of clients, …)
