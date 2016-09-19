# Ray Scheduler Prototype

## Overview

Aims of the scheduler prototype include the following:

- Same APIs as a production scheduler
- Easy to add new algorithms
- Able to replay production logs
- Support unit tests for core scheduler and for database (Redis) integration

## Usage

```
for trace in $(ls traces/*.json); do python replaytrace.py $trace; done
```
