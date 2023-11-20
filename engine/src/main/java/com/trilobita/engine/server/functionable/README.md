# Agregator
## PageRank
a PageRank algorithm would run until convergence was achieved, and aggregators would be useful for detecting the convergence condition.

## Implementation
An aggregator (Section 3.3) computes a single global value by applying an aggregation function to a set of values that the user supplies. 
Each worker maintains a collection of aggregator instances, identified by a type name and instance name. When a worker executes a superstep for any partition of the graph, the worker combines all of the values supplied to an aggregator instance into a single local value: an aggregator that is partially reduced over all of the worker’s vertices in the partition. At the end of the superstep workers form a tree to reduce partially reduced aggregators into global values and deliver them to the master. We use a tree-based reduction—rather than pipelining with a chain of workers—to parallelize the use of CPU during reduction.
The master sends the global values to all workers at the beginning of the next superstep.

## overview
Pregel aggregators are a mechanism for global communication, monitoring, and data. Each vertex can provide a value to an aggregator in superstep S, the system combines those values using a reduction operator, and the resulting value is made available to all vertices in superstep S + 1. Pregel includes a number of predefined aggregators, such as min, max, or sum operations on various integer or string types. Aggregators can be used for statistics. For instance, a sum aggregator applied to the out-degree of each vertex yields the total number of edges in the graph. More complex reduction operators can generate histograms of a statistic.
Aggregators can also be used for global coordination. For instance, one branch of Compute() can be executed for the supersteps until an and aggregator determines that all vertices satisfy some condition, and then another branch can be executed until termination. A min or max aggregator, applied to the vertex ID, can be used to select a vertex to play a distinguished role in an algorithm.
To define a new aggregator, a user subclasses the predefined Aggregator class, and specifies how the aggregated value is initialized from the first input value and how multiple partially aggregated values are reduced to one. Aggregation operators should be commutative and associative.
<!-- By default an aggregator only reduces input values from a single superstep, but it is also possible to define a sticky aggregator that uses input values from all supersteps. This is useful, for example, for maintaining a global edge count that is adjusted only when edges are added or removed. More advanced uses are possible. For example, an aggregator can be used to implement a distributed priority queue for the ∆-stepping shortest paths algorithm [37]. Each vertex is assigned to a priority bucket based on its tentative
distance. In one superstep, the vertices contribute their indices to a min aggregator. The minimum is broadcast to all workers in the next superstep, and the vertices in the lowest-index bucket relax edges. -->

## Master  
Fault tolerance is achieved through checkpointing. At the beginning of a superstep,the master separately saves the aggregator values.

The master also maintains statistics about the progress of computation and the state of the graph, such as the total size of the graph, a histogram of its distribution of out-degrees, the number of active vertices, the timing and message traffic of recent supersteps, and the values of all user-defined aggregators. To enable user monitoring, the master runs an HTTP server that displays this information.