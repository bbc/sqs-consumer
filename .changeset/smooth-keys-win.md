---
"sqs-consumer": patch
---

Stop message heartbeats when the handler settles so new visibility extensions cannot race message deletion or visibility termination.
