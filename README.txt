Introduction:
The goal of query optimization is to reduce the number of system resources needed to fulfill a 
query. This will ultimately lead to faster delivery of the correct results to the user. In addition, 
the application appears more responsive to the user because it provides faster results.
In this project, we will create a simple query optimizer that evaluates the query processing 
costs for a given SQL query and its optimized rewritten query. This optimizer will select the 
most efficient execution plan.
Design:
Two queries Q1 and RQ1 are taken. RQ1 is the optimized Rewritten Query of Q1. The optimal 
query processing plan is found by evaluating all possible execution plans for each Q1 and RQ1. 
In the end, we compare the costs to determine the least expensive plan.
We define the followings data in the program:
- Page Size: 4096 Byte
- Block Size: 100 pages
- Table Size:
T1: Each tuple is 20 bytes long = 1000 pages.
T2: Each tuple is 40 bytes long = 500 pages.
T3: Each tuple is 100 bytes long =2000 pages.
- Query Processing Cost = Disk I/O Cost + CPU Computation Cost,
- Disk I/O Cost = Disk Access Time + Data Transfer Time,
- Disk Access Time = Seek Time + Latency,
- Assume Data Transfer rate = 45 MB/sec,
- Average Seek time = 8 ms,
- Average Latency = 4 ms,
- Query Processing Cost = Disk I/O Cost 
= # of Disk I/O * Disk Access Time 
= # of Disk I/O * (8 ms + 4 ms) 
= Total # of Disk Block access needed * 12ms 
For each cost calculated at the end of loop we convert this cost into time in Hour/Min/Sec.
