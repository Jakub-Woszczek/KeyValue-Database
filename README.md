# Key-Value Database in Go (WIP)


## Overview

A custom **Key–Value NoSQL database** in Go (from scratch), inspired by LevelDB/RocksDB, using **LSM Tree** architecture. 

## DB goals

* Store data as key-value pairs with `PUT`, `GET`, `DELETE`.
* In-memory **MemTable** with periodic flush to disk **SSTables**.
* **Write-Ahead Log** for durability and crash recovery.
* **Bloom Filters** and **compaction** for efficient reads and storage.