# Project 1 — Incremental Sales Data Pipeline
## Azure Data Factory · Azure SQL Database

---

## Overview

An incremental ELT pipeline built using **Azure Data Factory** that loads only new or updated records from a source Azure SQL Database table into a destination reporting table, using a **watermark-based change tracking** pattern.

---

## Pipeline Architecture

```
Azure SQL DB (SalesData — source)
        |
        v
Lookup 1 — checklastmoddate
(Get last loaded watermark timestamp)
        |
        v
Lookup 2 — Newwatermark
(Get MAX(UpdatedAt) from source table)
        |
        v
Copy Data — copyingnewdata
(Copy only records between watermark and new watermark)
        |
        v
Script — Script1
(Update Watermark table with new timestamp)
```

---

## Pipeline Activities

### Activity 1 — Lookup: checklastmoddate
Reads the last loaded timestamp from the Watermark table.

```sql
SELECT LastLoadedAt
FROM Watermark
WHERE TableName = 'SalesData'
```

- Source dataset: `batman_watermark`
- First row only: Yes
- Purpose: Get the starting point for incremental load

---

### Activity 2 — Lookup: Newwatermark
Gets the latest updated timestamp from the source table.

```sql
SELECT MAX(UpdatedAt) AS newwatermark
FROM SalesData
```

- Source dataset: `batman_salesdata`
- First row only: Yes
- Purpose: Get the ending point for incremental load

---

### Activity 3 — Copy Data: copyingnewdata
Copies only records updated between the old and new watermark.

```sql
@concat(
  'Select * from SalesData where UpdatedAt > ''',
  activity('checklastmoddate').output.firstRow.LastLoadedAt,
  ''' AND UpdatedAt <= ''',
  activity('Newwatermark').output.firstRow.newwatermark,
  ''''
)
```

- Source dataset: `batman_salesdata`
- Destination: Reporting table in Azure SQL DB
- Purpose: Load only new/changed records — avoids full table reload

---

### Activity 4 — Script: Script1
Updates the Watermark table with the new timestamp after successful load.

```sql
@concat(
  'UPDATE Watermark SET LastLoadedAt = ''',
  activity('Newwatermark').output.firstRow.newwatermark,
  ''''
)
```

- Linked service: `doom_batman`
- Purpose: Move watermark forward so next run starts from here

---

## Why Watermark-Based Incremental Load?

| Approach | How it works | Problem |
|----------|-------------|---------|
| Full Load | Copy entire table every run | Slow, expensive, wasteful |
| Incremental Load | Copy only new/changed records | Fast, efficient, production-grade |

The watermark pattern ensures:
- Only new records are processed each run
- No duplicate data
- Efficient use of compute and storage
- Safe to re-run — idempotent

---

## Pipeline Run — Success

All 4 activities completed successfully:

| Activity | Type | Duration | Status |
|----------|------|----------|--------|
| checklastmoddate | Lookup | 9s | Succeeded |
| Newwatermark | Lookup | 15s | Succeeded |
| copyingnewdata | Copy data | 20s | Succeeded |
| Script1 | Script | 11s | Succeeded |

---

## Screenshots

| Screenshot | Description |
|-----------|-------------|
| pipeline_executions | All 4 activities succeeded |
| lookup_activity_1 | Lookup 1 — watermark query settings |
| lookup_activity_2 | Lookup 2 — MAX(UpdatedAt) query |
| copy_activity | Copy activity — incremental query with dynamic content |
| script_to_update_watermark_table | Script activity — watermark update query |

![Pipeline Executions](pipeline_executions.png)
![Lookup Activity 1](lookup_activity_1.png)
![Lookup Activity 2](lookup_activity_2.png)
![Copy Activity](copy_activity.png)
![Script to Update Watermark Table](script_to_update_watermark_table.png)

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Data Factory | Pipeline orchestration |
| Azure SQL Database | Source and destination |
| Watermark Table | Track last loaded timestamp |
| Dynamic Content | Build incremental SQL query at runtime |

---

## Author
**Arjun Tamilselvan**
Cloud Engineer 
