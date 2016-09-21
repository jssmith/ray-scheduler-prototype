# Ray Scheduler Prototype

## Overview

Aims of the scheduler prototype include the following:

- Same APIs as a production scheduler
- Easy to add new algorithms
- Able to replay production logs
- Support unit tests for core scheduler and for database (Redis) integration

## Usage Example

```
(NUM_NODES=2
NUM_WORKERS_PER_NODE=5
for trace in $(ls traces/*.json); do
    python replaytrace.py $NUM_NODES $NUM_WORKERS_PER_NODE $trace;
done)
```
