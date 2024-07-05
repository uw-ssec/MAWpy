# Developing Workflows

restrictions:

- STAYINT is only applied if a user's data comprises data from different sources
  (e.g., GPS and cellular) with very different uncertainty radius. In this case,
  DP shall be applied first.
- STAYCAL or STAYINT cannot be applied prior to the use of either TRACESEG or
  INCREMENTAL
- OSC can be applied before TRACESEG or INCREMENTAL
- TRACESEG AND INCREMENTAL can be applied together, but can only in one order:
  TRACESEG first followed by INCREMENTAL, not the other way
- neither TRACESEG or INCREMENTAL shall be applied more than once
- TIME4OSC, STAY_SP_THRESHOLD, STAY_TIME_THRESHOLD, are diagnosis tools used to
  detect appropriate thresholds to be used in OSC, TRACESEG, INCREMENTAL

## Sample Workflows

fill in the below based on the MAW paper.

### Workflow 1

### Workflow 2

### Workflow 3

### Workflow 4

### Workflow 5

### Workflow 6
