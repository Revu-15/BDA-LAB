# BDA LAB EXPERIMENT 1 REPORT
## 1. Objective
To use Hadoop to explore large-scale datasets stored in the Hadoop Distributed File System (HDFS), and perform basic operations such as listing files, reading data, and calculating summary statistics.

---

## 2. Theoretical Overview: HDFS Architecture
Hadoop Distributed File System (HDFS) is a Java-based distributed, fault-tolerant, and scalable filesystem designed to run on commodity hardware.

```
                  +--------------------------------+
                  |            NameNode            |
                  |     (Metadata & Namespace)      |
                  +--------------------------------+
                                  |
         +------------------------+------------------------+
         |                                                 |
         v                                                 v
+------------------+                              +------------------+
|     DataNode 1   |                              |     DataNode 2   |
|   (Block: blk_1) |<============================>|   (Block: blk_1) |
+------------------+       Replication Sync       +------------------+
```

### Key Components of HDFS:
1. **NameNode**:
   - The master server that manages the filesystem namespace, tree structure, and metadata for all files/directories.
   - It does not store actual data blocks; instead, it maintains mappings of files to blocks and blocks to DataNodes.
   
2. **DataNode**:
   - The worker nodes that store and retrieve the actual blocks of data as directed by the NameNode.
   - They periodically report their active block list to the NameNode via Heartbeats.

3. **Blocks**:
   - Files in HDFS are split into large, fixed-size segments called blocks (default: 128 MB in Hadoop 2.x/3.x).
   - Splitting files enables parallel processing and accommodates files larger than any single disk.

4. **Replication Factor**:
   - To ensure fault tolerance and high availability, each block is replicated across multiple DataNodes (default replication factor: 3).
   - This ensures data is not lost even if a DataNode fails.

---

## 3. Hadoop Shell Commands Reference
The command-line interface to interact with HDFS is invoked via `hdfs dfs` or `hadoop fs`.

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **List Files** | `hdfs dfs -ls <hdfs_path>` | Lists the files and subdirectories in the specified directory. |
| **Make Directory** | `hdfs dfs -mkdir <hdfs_path>` | Creates a new directory in HDFS. |
| **Upload File** | `hdfs dfs -put <local_src> <hdfs_dst>` | Copies a file from the local file system to HDFS. |
| **Display File Content** | `hdfs dfs -cat <hdfs_file>` | Outputs the contents of the file to stdout. |
| **Delete File/Dir** | `hdfs dfs -rm [-r] <hdfs_path>` | Deletes files or directories (`-r` is recursive delete). |

---

## 4. Implementation Details
Because Hadoop requires a full Java/Linux setup (often tricky to configure on native Windows), the lab workspace has been equipped with a persistent **HDFS and Hadoop CLI Simulator** in Python.

All operations are consolidated within a single file:
- **`hadoop_exploration.py`**: Includes the transaction sales data generator (`sales_data.csv`), NameNode namespace block allocation metadata manager, DataNode local block store simulator, standard shell command interface (`ls`, `mkdir`, `put`, `cat`, `rm`), and a MapReduce data exploration analytics engine.

---

## 5. Walkthrough of HDFS Simulator Output

### Step 1: Initialize HDFS Environment & Create Directories
The HDFS namespace metadata is managed in `.hdfs_metadata.json` and block store is housed in `.hdfs_data/`.

```bash
[Actual Hadoop Command]: hdfs dfs -mkdir -p /user
[NameNode] Processing mkdir request for: /user
[NameNode] Successfully created directory: /user

[Actual Hadoop Command]: hdfs dfs -mkdir -p /user/hadoop/sales
[NameNode] Processing mkdir request for: /user/hadoop/sales
[NameNode] Successfully created directory: /user/hadoop/sales
```

### Step 2: Upload File (Block Splitting and Replication Allocation)
When uploading `sales_data.csv` (~850 KB), HDFS determines the number of blocks needed and maps their replicas.

```bash
[Actual Hadoop Command]: hdfs dfs -put sales_data.csv /user/hadoop/sales
[NameNode] Initiating file write for '/user/hadoop/sales/sales_data.csv' (849627 bytes)
[NameNode] Block size configuration: 128 MB (134217728 bytes)
[NameNode] Splitting file into 1 simulated block(s)...
  -> Block 1/1 (blk_100000): Allocated replication to datanode-1, datanode-2, datanode-3
[NameNode] Block allocation and metadata updated.
[DataNode] Blocks successfully stored and replicated across cluster.
```

### Step 3: List Files in HDFS
Listing confirms permissions (`-rw-r--r--`), replica count (`3`), file size, modification date, and HDFS logical path.

```bash
[Actual Hadoop Command]: hdfs dfs -ls /user/hadoop/sales
Found 5 items
Permissions  Repl Owner    Group      Size (Bytes) Modification Date   Path
--------------------------------------------------------------------------------
-rw-r--r--   3    hadoop   supergroup 849627       2026-07-17 13:14:30 /user/hadoop/sales/sales_data.csv
```

### Step 4: Stream HDFS File Contents (`cat`)
Data client requests block location from NameNode, reads from the nearest replica, and outputs content.

```bash
[Actual Hadoop Command]: hdfs dfs -cat /user/hadoop/sales/sales_data.csv
[NameNode] Retrieving block list for: /user/hadoop/sales/sales_data.csv
  -> Reading blk_100000 from replica datanode-1

=== Streaming data from HDFS DataNodes (showing first 10 lines) ===
TransactionID,Timestamp,CustomerID,Age,Gender,Category,Quantity,UnitPrice,TotalAmount,PaymentMethod
TX000001,2026-07-19 03:00:05,CUST3228,49,Male,Electronics,5,389.88,1949.4,UPI
TX000002,2026-02-14 06:14:12,CUST1981,42,Other,Books,2,6.4,12.8,Credit Card
TX000003,2026-02-26 14:04:18,CUST1194,59,Male,Books,1,21.06,21.06,UPI
TX000004,2026-02-17 06:20:58,CUST8091,28,Other,Beauty & Health,2,55.64,111.28,Debit Card
TX000005,2026-02-11 12:51:10,CUST2304,38,Male,Electronics,3,167.6,502.8,Net Banking
...
```

---

## 6. Calculated Summary Statistics
A MapReduce/Spark data analysis job executes over the logical HDFS filepath, reducing intermediate block calculations to produce summary statistics:

### Descriptive Statistics for Numeric Attributes
* **Total Records Count**: 10,000

| Metric | Age | Quantity | UnitPrice ($) | TotalAmount ($) |
| :--- | :--- | :--- | :--- | :--- |
| **Count** | 10000 | 10000 | 10000 | 10000 |
| **Mean** | 46.4796 | 2.9929 | 203.4750 | 605.7239 |
| **Std Dev** | 16.7732 | 1.4230 | 257.7008 | 887.5386 |
| **Min** | 18 | 1 | 5.01 | 5.01 |
| **25%** | 32.0 | 2.0 | 41.76 | 103.135 |
| **50% (Median)** | 46.0 | 3.0 | 100.35 | 260.79 |
| **75%** | 61.0 | 4.0 | 251.9325 | 693.30 |
| **Max** | 75 | 5 | 1199.18 | 5970.80 |

### Categorical Distributions (Top Frequent Values)
1. **Gender**:
   - Male: 3,439 (34.39%)
   - Other: 3,289 (32.89%)
   - Female: 3,272 (32.72%)
2. **Category**:
   - Clothing: 1,684 (16.84%)
   - Electronics: 1,683 (16.83%)
   - Home & Kitchen: 1,675 (16.75%)
3. **PaymentMethod**:
   - PayPal: 2,057 (20.57%)
   - Credit Card: 2,012 (20.12%)
   - Debit Card: 2,004 (20.04%)

---

## 7. Conclusion
Through this experiment, the HDFS architecture of NameNode and DataNodes was successfully explored. The dataset was written to simulated HDFS, split into blocks, and replicated across the simulated cluster. Data exploration operations (listing directory, viewing file head, and calculating MapReduce statistics) were completed successfully.
