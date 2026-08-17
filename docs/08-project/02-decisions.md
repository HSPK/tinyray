# Decisions

> Proposal; not the current implementation.

> Each decision with its reason and its cost. Reversals are included: a design
> that never changed its mind was not tested against reality.

## 1. Scope decisions

### tinyray occupies L2 only

**Why.** L1 is solved by schedulers with far more operational history. L3 is the
product. L2 is what every large control plane writes by hand — one proposal
reviewed for this work contained **15 identity types**, each needing its own
generation and fencing check.

**Cost.** tinyray is a library, not a runtime. It will not run your job.

### No resource management

**Why.** Slurm or Kubernetes allocated the GPUs before tinyray was imported. A
second ledger can only disagree with the first.

**Cost.** tinyray cannot prevent two processes claiming one device. That
protection moves to the scheduler, where it is stronger.

### No launching, except within a node

**Why.** `torchrun`, `srun` and Kubernetes own `__main__`.

**Cost.** No gang start. `wait_ready(size)` refuses to proceed until the
launcher has delivered, which gives the same guarantee where it matters.

### No data plane

**Why.** NCCL, UCX and NIXL are better at it and already present.

**Cost.** tinyray cannot make a slow data path fast.

## 2. Mechanism decisions

### Self-registration, not roster push

**Why.** **Measured**: a roster push is O(N) calls carrying O(N) bytes — 2.3 GB
out of one process at 8,192 workers. Self-registration is O(1) per worker, issued
from ten thousand places instead of arriving at one.

**Cost.** Membership is eventually consistent. Fencing makes that safe.

### Hierarchical leases

**Why.** Kubernetes supports 5,000 nodes and documents node leases as a source
of etcd pressure. Ten thousand worker leases is twice that budget on top of the
cluster's own. Workers lease against their cell; cells lease against consensus.
**Derived**: 7.8 consensus writes/s instead of 1,000.

**Cost.** Death reaches the global tier in worker TTL plus one cell interval.

### Soft state replicated without consensus

**Why.** Every membership record is re-asserted by its owner each heartbeat, so a
replica that loses everything is correct one lease later. There is no history to
agree about.

**Cost.** A replica can serve a stale answer. Fencing makes it safe.

### Reads fall back to cache

**Why.** **Measured**: with every replica killed, workers continued addressing
each other and work continued. The failure that matters is not a lost record but
a stopped job.

**Cost.** Staleness is unbounded during an outage.

### Scoped lookup

**Why.** A worker needs its four peers, not ten thousand endpoints. Filtering
server-side keeps the response bounded by the request.

**Cost.** The caller must know its scope. tinyray will not compute it, because
that mapping is a parallelism decision.

### Fencing in the transport, not at call sites

**Why.** Fifteen hand-written checks is fifteen chances to write the one that
always passes.

**Cost.** Every call carries an incarnation, and a legitimate caller with a stale
lookup is rejected and must re-look-up.

### Only backpressure is retried

**Why.** It is the one outcome where the call provably did not run. Everything
else is a fact about state, and replaying a stateful call because it failed would
apply it twice.

**Cost.** No `max_task_retries`. Application-level retry is the application's.

### Linear backoff

**Why.** The peer is draining a queue, not collapsing. Exponential backoff
overshoots a queue that clears in milliseconds.

### A superseded process is not killed

**Why.** A library calling `os._exit` inside a training job is worse than the
problem it solves. Supersession already stops the addressing; the callback exists
for applications that want more.

**Cost.** A superseded process keeps running, holding whatever it holds. That is
L1's and the application's.

## 3. Reversals

The ones that were wrong first.

### Star topology to peer mesh, and then to hierarchy

**Originally** the driver was at the centre and relayed every message.

**Reversed because** that is the shape of a fan-out, not a pipeline. A mesh was
built next — and was still wrong at scale, because introducing N workers to each
other is quadratic. The third answer is hierarchy with scoped lookup: workers
learn only the peers they need.

**Lesson.** The first fix addressed the topology and kept the assumption that
everyone must know everyone.

### tinyray owns the process, to tinyray supervises it, to the launcher owns it

**Originally** actor classes, Ray-style, with tinyray owning the interpreter.
Then supervision of processes it started. Now: the launcher starts everything,
and supervision is a node-local option.

**Lesson.** Each step gave up ownership, and each was prompted by a real
framework refusing to give it up.

### Placement, then no placement

**Originally** a resource table, gang placement and device assignment, with
tests. All of it is deleted.

**Why.** At the target scale the scheduler has already done it. The code was
correct and answering a question nobody was asking.

### Shared registry identity, reverted immediately

**Originally** registry replicas were given one fixed identity to save a
discovery round trip.

**Reversed because** clients route by identity: two replicas sharing one meant
calls were submitted to one and fetched from the other. Every single-replica
test passed.

**Lesson.** Identity is not an optimisation site, and an availability feature
tested with one instance is untested.

### Membership version bumped by heartbeat, then not

**Originally** any registry write moved the version.

**Reversed because** a watcher would then re-fetch once per heartbeat per
worker — a quadratic hidden inside a protocol that looks linear.

## 4. Rejected alternatives

| Rejected | Why |
|---|---|
| Everything in etcd | Melts at ten thousand lease holders |
| Nothing in consensus | Leadership and ownership have no owner to re-assert them |
| Raft for membership | Nothing to agree about; it regenerates |
| Gossip for membership | Convergence is harder to reason about than re-assertion, for no gain when every fact has an owner |
| Occupying L1 as a `RuntimeBackend` | Already solved; reintroduces resource ownership |
| gRPC | A protobuf toolchain and a code generator for point-to-point control traffic that needs neither. HTTP is debuggable with `curl` |
| Push-based watch, initially | Polling a version costs a few bytes on a stable cluster. On the roadmap, not urgent |
| Killing a superseded process | Too aggressive for a library |

## 5. Decisions still open

| Question | Options | Blocked on |
|---|---|---|
| Cell size | 64 / 128 / 256 GPU, or the communicator scope | Fabric topology and failure data |
| Consensus store | etcd, or an existing Kubernetes API | Deployment environment |
| Broadcast at 10k | Scoped only, thread-pooled, or tree fan-out | Whether a real broadcast is needed per iteration |
| Authentication | mTLS, tokens, or network isolation only | Whether the cluster is shared |
| Admission weighting | Count, bytes, or estimated duration | Real workload measurement |

Each is recorded rather than guessed. A default chosen without the measurement
becomes permanent by accident.
