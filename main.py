import os
import re
import glob
import time
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(BASE_DIR, 'sql')

def find_sql_files(folder_path):
    files = []
    # **/* untuk mencari di folder dan subfolder
    search_path = os.path.join(folder_path, "**", "*.sql") 
    
    # urutkan hasil glob agar konsisten
    found_files = sorted(glob.glob(search_path, recursive=True))
    
    for filename in found_files:
        if "/source/" in filename.replace("\\", "/"): 
            continue
        files.append(filename)
    return files

# --- 2. EKSTRAK DEPENDENSI (PERBAIKAN REGEX) ---
def extract_dependencies(file_path):
    dependencies = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read().lower()
            
            matches = re.findall(r'(tmp|final)\.(\w+)', content)
            
            for schema, table in matches:
                dependencies.add(f"{schema}.{table}")
    except Exception as e:
        print(f"Error membaca {file_path}: {e}")
        
    return list(sorted(dependencies))

# SORTING ALGORITHM 
def get_sorted_execution_order(file_deps):
    all_nodes = set(file_deps.keys())
    
    # Bangun Graph (Urutkan keys agar stabil)
    graph = {node: [] for node in all_nodes}
    in_degree = {node: 0 for node in all_nodes}
    
    for node in sorted(file_deps.keys()):
        info = file_deps[node]
        for dep in sorted(info['depends_on']):
            if dep in all_nodes:
                graph[dep].append(node)
                in_degree[node] += 1
    
    queue = sorted([node for node in all_nodes if in_degree[node] == 0])
    sorted_order = []
    
    while queue:
        current = queue.pop(0)
        sorted_order.append(current)
        
        newly_ready = []
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                newly_ready.append(neighbor)
        
        queue.extend(sorted(newly_ready))
                
    if len(sorted_order) != len(all_nodes):
        print(" PERINGATAN: Ada Circular Dependency!")
        
    return sorted_order
#simulasi task 
def run_task(task_name):
    """Fungsi simulasi menjalankan SQL dengan delay"""
    print(f"    [Mulai] {task_name}...")
    time.sleep(2) # Simulasi proses berat (2 detik)
    print(f"    [Selesai] {task_name}")
    return task_name

# PIPELINE RUNNER ---
def execute_pipeline(file_map):
    print("\n--- LAPORAN DETEKSI DEPENDENSI ---")
    files_with_deps = 0
    
    # 1. Bangun Graph Ulang (Khusus untuk Runner)
    all_nodes = set(file_map.keys())
    graph = {node: [] for node in all_nodes}
    in_degree = {node: 0 for node in all_nodes}
    
    for node in sorted(file_map.keys()):
        info = file_map[node]
        deps = [d for d in info['depends_on'] if d in file_map]
        
        if deps:
            files_with_deps += 1
            print(f"🔗 {node} \tBUTUH: {deps}")
            
        for dep in deps:
            graph[dep].append(node)
            in_degree[node] += 1

    if files_with_deps == 0:
        print(" PERINGATAN: Tidak ada dependensi terdeteksi!")
    else:
        print(f" Deteksi Sukses: {files_with_deps} file memiliki ketergantungan.")
    print("-" * 40)
    print(" MEMULAI PIPELINE PARALEL...\n")

    total_executed = 0
    start_time = time.time()
    
    # 2. Loop Eksekusi per Level (Batching)
    while total_executed < len(all_nodes):
        # Cari semua tugas yang SIAP JALAN (in_degree == 0) SAAT INI
        ready_tasks = [node for node in all_nodes if in_degree[node] == 0]
        
        if not ready_tasks:
            print(" ERROR: Deadlock terdeteksi! Sisa file saling menunggu.")
            break
            
        ready_tasks.sort() # Sort agar log rapi
        print(f" Batch Baru: {ready_tasks}")
        
        # Menjalankan semua ready_tasks secara bersamaan
        with ThreadPoolExecutor() as executor:
            executor.map(run_task, ready_tasks)
            
        # 3. Update Status setelah Batch selesai
        for task in ready_tasks:
            all_nodes.remove(task) # Hapus dari antrian
            total_executed += 1
            
            # Kabari tetangga: "Saya sudah selesai"
            for neighbor in graph[task]:
                in_degree[neighbor] -= 1

    end_time = time.time()
    print(f"\n SEMUA SELESAI dalam {end_time - start_time:.2f} detik.")

# --- MAIN BLOCK ---
if __name__ == "__main__":
    if not os.path.exists(SQL_PATH):
        print(f" Folder tidak ditemukan di: {SQL_PATH}")
    else:
        # [FIX] Panggil fungsi find_sql_files (sesuai definisi di atas)
        all_files = find_sql_files(SQL_PATH)
        file_map = {}

        for file_path in all_files:
            norm_path = os.path.normpath(file_path)
            parts = norm_path.split(os.sep)
            schema = parts[-2]
            basename = os.path.splitext(parts[-1])[0]
            unique_id = f"{schema}.{basename}".lower()
            
            file_map[unique_id] = {
                "path": file_path,
                "depends_on": extract_dependencies(file_path)
            }
        
        execute_pipeline(file_map)