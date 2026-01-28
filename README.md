# Distributed Trainer

A distributed training runtime focused on **explicit orchestration**, **coordinated checkpointing**, and **crash-safe recovery**.

This project explores how modern distributed training systems are built at the control-plane level: how workers are coordinated, how checkpoints are made consistent, and how jobs recover from partial failure. The emphasis is on **distributed systems correctness**, not ML model novelty or peak performance.

---

## Overview

The system is a **job-oriented distributed runtime** composed of a centralized controller and shared-nothing worker replicas.

* A **coordinator** acts as the job controller and owns all global decisions.
* **Worker agents** execute training, report observed state, and perform checkpoint I/O.
* **Durable storage** is used for checkpoint data and metadata.
* **gRPC is used only as a transport layer**; all orchestration logic lives above it.

The architecture is intentionally minimal and explicit, mirroring patterns used in systems like Spark, Ray, and Kubernetes controllers.

---

## Architecture

### Control Plane

* Centralized coordinator.
* Maintains desired job state (membership, lifecycle, checkpoints).
* Continuously reconciles desired state against observed worker state.
* Commits checkpoints and drives recovery.

### Execution Plane

* Worker agents run on each node/process.
* Execute commands from the coordinator.
* Supervise the training process.
* Maintain a small local state machine and journal to ensure idempotent execution.

### Storage Plane

* External durable storage (local FS, MinIO, or S3-like).
* Stores checkpoint blobs and commit markers.
* Treated as a service, not shared disk.

---

## Core ideas

### Job-oriented orchestration

Workers are not independent clients. They are replicas participating in a **single training job**. The primary abstraction is the job, not individual RPCs.

### Desired state vs actual state

* The coordinator tracks **desired state**.
* Workers report **actual state**.
* The coordinator issues commands until the system converges.

All decisions are based on **observed facts**, never assumptions.

### Coordinated checkpoints

Checkpoints are treated as transactions:

1. Coordinator issues a checkpoint command.
2. Workers write checkpoint blobs.
3. Workers acknowledge completion.
4. Coordinator commits the checkpoint in durable metadata.

Only **committed checkpoints** are considered recoverable.

### Crash-only components

* Workers and coordinator may crash and restart at any time.
* Operations are idempotent.
* Recovery is driven entirely by durable state and reported facts.

---

## Repository layout

```text
distributed-trainer/
├── proto/
│   └── agent.proto          # gRPC + worker state schema
├── crates/
│   ├── common/              # shared types and utilities
│   ├── coordinator/         # orchestration + reconciliation logic
│   ├── worker-agent/        # command execution + local state
│   └── storage/             # object store abstraction
├── python-trainer/          # optional: real training implementation
├── scripts/                 # dev/testing helpers
├── Cargo.toml               # workspace definition
└── README.md
```

### Crate responsibilities

* `coordinator`: desired state, reconciliation loop, metadata log
* `worker-agent`: actual state, command execution, checkpoint I/O
* `storage`: durable blob interface (no job logic)
* `common`: pure utilities (no networking or I/O)

---

## Worker state reporting

Workers periodically send a **structured state snapshot** containing:

* identity and role
* current phase (training, checkpointing, restoring, etc.)
* last completed training position
* status of the most recent command
* checkpoint facts that are durably true

This snapshot is the sole input to the coordinator’s reconciliation logic.

---

## Status

Early development / architecture phase.

Initial milestones:

* Coordinator consumes worker state snapshots
* Deterministic reconciliation loop
* Safe no-op command execution
* Coordinated checkpoint protocol
* Crash/restart correctness
