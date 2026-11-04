"""
core/locks.py - In-process serialization for binding mutations.

Allocation and cooldown decisions follow a read-modify-write pattern:
read current occupancy/cooldowns, choose a node, then persist the binding.
FastAPI runs `def` endpoints in a threadpool, so two HTTP requests can execute
that sequence concurrently and both observe the same pre-write state, breaking
`max_accounts_per_ip` and the registration cooldown guarantee.

Every writer of `account_allocations` / `leases` / `ip_cooldowns` must hold this
lock for the whole read-modify-write window. The deployment is a single uvicorn
worker, so an in-process lock is sufficient; multi-worker deployments would need
`BEGIN IMMEDIATE` transactions or database-level constraints instead.
"""

import threading

BINDING_WRITE_LOCK = threading.RLock()
