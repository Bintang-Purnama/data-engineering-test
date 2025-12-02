# Data Engineering Technical Test - Solution

## 📋 Overview
This repository contains the solution for the ELT Data Pipeline technical test. The program is designed to orchestrate the execution of SQL files by automatically detecting their dependencies and executing them in the correct topological order.

**Key Features:**
* **Automatic Dependency Detection:** Uses Regex to parse SQL files and identify table dependencies (`tmp.*` or `final.*`).
* **Topological Sorting:** Determines the correct execution order (DAG) to ensure tables are created before they are queried.
* **Parallel Execution (Bonus):** Implements a "Level-by-Level" batch execution using `ThreadPoolExecutor`, allowing independent tasks to run simultaneously for maximum efficiency.
* **Robust Unit Testing:** Includes tests for dependency parsing and sorting logic.

## 🛠️ Prerequisites
* Python 3.11
* No external libraries required (uses only standard libraries: `os`, `re`, `glob`, `time`, `concurrent.futures`, `unittest`).

## 🚀 How to Run

### 1. Run the Data Pipeline (Main Program)
To execute the SQL pipeline with parallel processing simulation:

```bash
python main.py

### 2. Run Unit Test 
To verify th logic ( Regex parser & Sorting Algorithm ):
python tests.py

#🧠 Code Explanation
main.py (Orchestrator)
extract_dependencies(file_path):

Reads the SQL file content.

Uses Regex r'(tmp|final)\.(\w+)' to find table references.

Returns a list of dependencies (e.g., ['tmp.agents', 'tmp.photos']).

execute_pipeline(file_map):

Implements Kahn's Algorithm for Topological Sorting dynamically.

Identifies tasks with in_degree = 0 (no pending dependencies).

Executes them in parallel using ThreadPoolExecutor.

Updates the dependency graph and repeats for the next batch.

tests.py (Unit Tests)
test_extract_dependencies: Creates a dummy SQL file to verify if the Regex correctly captures table names (handling case sensitivity and backticks).

test_topological_sort: Verifies the sorting logic using a mock dependency graph (A->B->C) to ensure the order is strictly C -> B -> A.

### Execution Logs 
--- LAPORAN DETEKSI DEPENDENSI ---
🔗 final.listings_performances  BUTUH: ['tmp.listing_performances', 'tmp.listings_features']
🔗 tmp.listing_performances     BUTUH: ['tmp.enquiries_per_day', 'tmp.page_clicks_per_day']
🔗 tmp.listings_features        BUTUH: ['tmp.agents', 'tmp.listing_photos_quality']  
 Deteksi Sukses: 3 file memiliki ketergantungan.
----------------------------------------
 MEMULAI PIPELINE PARALEL...

 Batch Baru: ['tmp.agents', 'tmp.enquiries_per_day', 'tmp.listing_photos_quality', 'tmp.page_clicks_per_day']
    [Mulai] tmp.agents...
    [Mulai] tmp.enquiries_per_day...
    [Mulai] tmp.listing_photos_quality...
    [Mulai] tmp.page_clicks_per_day...
    [Selesai] tmp.enquiries_per_day
    [Selesai] tmp.agents
    [Selesai] tmp.page_clicks_per_day    [Selesai] tmp.listing_photos_quality        


 SEMUA SELESAI dalam 2.00 detik.

# Test Output ( python tests.py)

--- [TEST 1] Regex Dependensi ---
   Hasil deteksi: ['final.summary', 'tmp.orders']

--- [TEST 2] Logika Pengurutan (Sequential) ---
   Hasil urutan: ['user', 'transaksi', 'laporan']
.
----------------------------------------------------------------------
Ran 2 tests in 0.002s

OK

-----------------------------###########################-----------------------
### 🎉 Selesai!
