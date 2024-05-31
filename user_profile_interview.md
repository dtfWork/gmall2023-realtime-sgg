# sgg:

### 1. What does the portrait system mainly do?

1) User information labeling

2) The application of labeled data (clustering, insight analysis)

3) How labels are modeled and what labels are there

​	Based on user needs, coordinated with the product manager to plan the four level labels together. The first two levels are classification, the third level is labels, and the fourth level is label values.

### 2. Overall Project Architecture

![img](D:\1-code\idea-space\gmall2023-realtime-sgg\user_profile_interview.assets\wps1.png)

### 3. Explain the scheduling process of label calculation

![img](D:\1-code\idea-space\gmall2023-realtime-sgg\user_profile_interview.assets\wps3.png)

### 4. Batch processing of the entire label

Four tasks:

(1) By writing SQL based on the business logic of each label, produce a single label table.

(2) [Merge label single tables into label wide tables.](https://github.com/dtfWork/gmall2023-realtime-sgg/tree/master/task-sql)

(3) Export the label width table to the label width table in Clickhouse.

(4) Dump the label table in Clickhouse into a Bitmap table.

Four tasks are completed by writing Spark programs. And through the profiling platform scheduling, adding new tags in the future only requires filling in the tag definition, SQL, and related parameters on the platform.

### 5. What are the functions of your portrait platform?

(1) Label Definition

(2) Label task settings

(3) Task scheduling

(4) Task monitoring

(5) Cluster creation and maintenance

(6) Crowd Insight

### 6. Have you ever done web application development and what functions have been implemented

(1) Portrait platform grouping

(2) Other functions of the portrait platform (optional)

(3) Real time data warehouse interface

### 7. Upstream and downstream of portrait platforms

(1) Upstream: Data warehouse system

(2) Downstream: Written into Redis and accessed by advertising and operational systems.

![image-20240531125146813](D:\1-code\idea-space\gmall2023-realtime-sgg\user_profile_interview.assets\image-20240531125146813.png)

### 8. BitMap principle and why it can improve performance

Bitmap is a binary set that identifies the existence of a value using 0 or 1.

When calculating the intersection of two sets, there is no need to traverse the two sets, just perform an AND operation on the bits. Whether it is a decrease in the number of comparisons (from O (N ^ 2) to O (N)) or an improvement in comparison methods (bit operations), both bring significant performance improvements.

Business scenario: Put the user ID set of each label in a Bitmap, and when performing group filtering by finding the intersection of multiple labels (such as female+post-90s), the intersection of the Bitmaps of the two labels can be calculated.

### 9. what technology do you use to solve ID-mapping question?from-self

