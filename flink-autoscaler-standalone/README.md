# Flink Autoscaler Standalone

## What's the autoscaler standalone?

`Flink Autoscaler Standalone` is an implementation of `Flink Autoscaler`, it runs as 
a separate java process. It computes the reasonable parallelism of all job vertices 
by monitoring the metrics, such as: processing rate, busy time, etc. Please see 
[FLIP-271](https://cwiki.apache.org/confluence/display/FLINK/FLIP-271%3A+Autoscaling) 
for an overview of how autoscaling works.

`Flink Autoscaler Standalone` rescales flink job in-place by rescale api of 
[FLIP-291](https://cwiki.apache.org/confluence/x/9pRbDg).

## How To Use

Currently, `Flink Autoscaler Standalone` only supports a single Flink cluster.
It can be any type of Flink cluster, includes: 

- Flink Standalone Cluster
- MiniCluster
- Flink yarn session cluster
- Flink yarn application cluster
- Flink kubernetes session cluster
- Flink kubernetes application cluster
- etc

You can start a Flink Streaming job with the following ConfigOptions.

```
# Enable Adaptvie scheduler to play the in-place rescaling.
jobmanager.scheduler : adaptive

# Enable autoscale and scaling
job.autoscaler.enabled : true
job.autoscaler.scaling.enabled : true
job.autoscaler.stabilization.interval : 1m
job.autoscaler.metrics.window : 3m
```

Note: In-place rescaling is only supported since Flink 1.18. Flink jobs before version 
1.18 cannot be scaled automatically, but you can view the ScalingReport in Log. 
ScalingReport will show the recommended parallelism for each vertex.

After the flink job starts, please start the StandaloneAutoscaler process by the 
following command. 

```
java -cp flink-autoscaler-standalone-1.7-SNAPSHOT.jar \
org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint \
--flinkClusterHost localhost \
--flinkClusterPort 8081
```

Updating the `flinkClusterHost` and `flinkClusterPort` based on your flink cluster. 
In general, the host and port are the same as Flink WebUI.

## Extensibility of autoscaler standalone

`Autoscaler` as a generic autoscaler component defines a series of generic interfaces:

- **AutoScalerEventHandler** : Handling autoscaler events, such as: ScalingReport, 
  AutoscalerError, etc. It logs events by default.
- **AutoScalerStateStore** : Storing all state during scaling. `InMemoryAutoScalerStateStore` 
  is the default implementation, it's based on the Java Heap, so the state will be discarded 
  after process restarts. We will implement persistent State Store in the future, such as
  : `JdbcAutoScalerStateStore`.
- **ScalingRealizer** : Applying scaling actions. `RescaleApiScalingRealizer` is the default
  implementation, it uses the Rescale API to apply parallelism changes.
- **JobAutoScalerContext** : Including all details related to the current job.

`Autoscaler Standalone` isn't responsible for job management, so it doesn't have job information.
`Autoscaler Standalone` defines the `JobListFetcher` interface in order to get the 
`JobAutoScalerContext` of the job. It has a control loop that periodically calls 
`JobListFetcher#fetch` to fetch the job list and scale these jobs.

Currently `FlinkClusterJobListFetcher` is the only implementation of the `JobListFetcher` 
interface, that's why `Flink Autoscaler Standalone` only supports a single Flink cluster so far.
We will implement `YarnJobListFetcher` in the future, `Flink Autoscaler Standalone` will call 
`YarnJobListFetcher#fetch` to fetch job list from yarn cluster periodically.
