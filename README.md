# Taskling.NET
API for batch and micro-batch tasks:
- Partioning of batches into blocks of work with guaranteed isolation between blocks across batches
- Recover from failures with automatic reprocessing
- Limiting the number of concurrent task executions (across servers)
- Critical sections across servers
- Standardised activity logging and alerting.

## Versions
Note that:
- version 1.X is a synchronous API in .NET 3.5
- version 2.X is .NET Standard 2.0/.NET 4.5 that uses async/await on all calls

## Taskling is a host agnostic library
Taskling helps you manage your data processing tasks whether they be hosted in web applications, console applications, Windows Services or Cloud (worker roles, Azure Functions etc).
It is not a framework or platform like Spark or Hadoop, it is just a nuget package you can add to any project.

## Partition data processing into Time Slices, Numeric Ranges, Lists or Custom Data Structures
### Ranges - Time Slices and Numeric Slices
Partitioning into ranges is possible when
- Your data is ordered temporally or numerically
- Tracking of work at a range block level is sufficient. 

The benefit of range blocks is that each batch or micro-batch can use the last block of the previous batch to determine the start of the current batch. So for a numeric block, if the id 13253424 was the end range of the last block then you can use that information and process data with ids of 13253425 and above for the current batch.

You can set the range block size according to your needs, though smaller often offers advantages:
- Smaller blocks are less likely to fail due to timeouts
- If a single data item causes a block to fail, then multiple smaller blocks will ensure that only one block of many fails and that small block can be automatically retried later.
- Small blocks when processed sequentially can reduce the memory footprint

Range blocks are treated as a single entity and either succeed or fail as a block. When you retry a block you will reprocess the whole block. So range blocks are suitable when you can store the results with a database transaction, or you write th results to a file (including FTP), any mechanism where the storage or action is a single atomic event. If you need to be able to track indivudal data items and/or retry individual items then List Blocks are more suitable. 

#### Time Slices (Date Range Blocks)
When your data has a date/time dimension that forms an ordered sequence, use Date Range Blocks to partition the data choosing a maximum time span for the blocks. With a partition size of 1 hour and 24 hours of data to process, you'll generate 24 blocks. Each block can then be processed by your logic sequentially or in parallel. 

Data with a Created or Modified Date column is an example of a good partition value. Date Range Blocks are not suitable for unordered date/time sequences.
Date Range blocks are tracked at a block level, their success or failure is logged as a whole. 

#### Numeric Slices (Numeric Range Blocks)
When your data is ordered by a numeric value, such as an auto-incrementing primary key, then you can partition the data into Numeric Range blocks choosing a maxium range size. With a range of 10000 items and a block size of 500, 20 blocks will be genrated. These blocks can then be processed by your logic sequentially or in parallel. 

### Lists (List Blocks)
Lists are suitable when one or more of the following match your situation
- Your data is unordered
- Your data is unstructured
- You need to be able to track individual data items.
- You want to only retry individual items that have failed.

List Blocks are partitioned on the number of items per block. So if you have 10000 items to process, and a block size of 500 then Taskling wil generate you 20 List Blocks each of 500 data items. List Blocks and their items can be processed in parallel.

A great example is sending emails. You partition your data (ordered or unordered) into lists of data items and need to send an email for each item. If a single item fails then when the block is reprocessed you need to guarantee that you don't resend the emails for the data items that succeeded. Because List Blocks track each individual item you can select only the failed items to reprocess.
When data is unordered you can load that unordered data into list blocks and process each data item. Depending on your data source, you may need a way of marking that data as processed so the next time the job runs it does not try and process it again.
 
### Custom Data Structures (Object Blocks)
Object Blocks use generic types to allow you to store any class in them. Under the hood the class instance you pass to Taskling gets converted to JSON for persistent storage.
Object Blocks are tracked at a block level. They are the block to use when range and list blocks don't match your situation.

## Auto-Recover from Failure
Taskling detects both Failed blocks and Dead blocks. You can configure a task to reprocess failed and dead blocks within a recent time period and up to a maximum number of retries. Each time your task executes Taskling will look for previous blocks that are dead or failed and return them with the new blocks. Let's look at an example.

Your batch task executes every hour with Date Range Blocks with the following configuration
 - a maximum time span of 15 minutes per block
 - maximum number of retries is 2, so that means three attempts in total can be made.
  
- Execution 1 starts at 01:00
	- Block 1 00:00 - 00:15 Ok
	- Block 2 00:15 - 00:30 Failed
	- Block 3 00:30 - 00:45 Failed
	- Block 4 00:00 - 01:00 Ok
	
- Execution 2 starts at 02:00
	- Block 2 00:15 - 00:30 Ok
	- Block 3 00:30 - 00:45 Failed
	- Block 5 01:00 - 01:15 Ok
	- Block 6 01:15 - 01:30 Ok
	- Block 7 01:30 - 01:45 Ok
	- Block 8 01:45 - 02:00 Ok
	
- Execution 3 starts at 03:01
	- Block 3 00:30 - 00:45 Failed
	- Block 9 02:00 - 02:15 Ok
	- Block 10 02:15 - 02:30 Ok
	- Block 11 02:30 - 02:30 Ok
	- Block 12 02:45 - 03:00 Ok
	- Block 13 03:00 - 03:01 Ok
	
- Execution 4 starts at 04:00
	- Block 14 03:01 - 03:16 Ok
	- Block 15 03:16 - 03:36 Ok
	- Block 16 03:36 - 03:46 Ok
	- Block 17 03:46 - 04:00 Ok
	
### Failed Blocks
When an exception occurs, or business logic mandates that a particular situation be marked as a failure then you call the Failed() method on the block. This tells Taskling that the block failed.

### Dead Blocks
Sometimes failures cannot be logged, such as when a ThreadAbortException occurs. In these situations even your log4net, NLog etc don't get the opportunity to log the demise of your batch task, the task simply dies without any warning or any evidence to suggest what happened. Taskling calls these Dead blocks. It detects dead blocks by using a Keep-Alive to signal that the task is running. When a task is in the In Progress state but the last Keep-Alive was past a configured time period Taskling knows that the task died.

### Reprocessing
Auto recovery via reprocessing works by executions looking for previous executions that failed. For tasks that execute frquently this is likely to be a sufficient time window for reprocessing.

If your task runs once per day then any reprocessing for that block will happen 24 hours later. If this is too late then you can
- run the task every X hours so that it detects a failure sooner. If it is the first task execution to run that day then it in processes the new data, if it is the second/third etc execution of the day then it only looks for failed and dead tasks, but does not process new data.
- create a second task that only looks for failed and dead blocks

## Limit the Number of Concurrent Executions
Some batch tasks need to be singletons. If you run your task every hour but it can take more than an hour to run then you could end up with two executions running. With Taskling you can configure a job to have a concurrency limit.

As well as singletons it can be useful to limit the number of concurrent executions to prevent the overloading of other components. May be you have massive amounts of data to process and so you run the task every minute and each execution takes ten minutes, you'll have ten concurrent executions. When a component in your architecture (web service, database etc) cannot handle the load of ten concurrent executions then you can put a limit of 5 for example. When components show signs of being overloaded you can simply reduce the concurrency limit in real-time and then increase it again later.

## Critical Sections 
Some tasks can have multiple concurrent executions but have sections of code that can only be executed by one execution at a time. For example, imagine you have two executions start around the same time. They both must identify the data they will process. If the data identification logic runs concurrently they could both identify the same data (files, database rows etc) and duplicate the processing. Critical sections around data identification logic will guarantee data isolation of different executions.

## Standardized Logging and Tracking
All the batch tasks that you build with Taskling will report their activity in the same way. This reduces the cost of ownership of the tasks because
- support teams know where to look for data regarding failing tasks
- monitoring teams can create standardized and scriptable alerts
- development teams can see behaviour of their tasks during development

Taskling enables the following alerts
- Failed/Dead blocks reach their retry limit
- X consecutive task executions have failed
- % of list block items have failed
