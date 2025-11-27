import unittest
import os
import sys

# --- KONFIGURASI PATH ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CURRENT_DIR)

from main import extract_dependencies, get_sorted_execution_order
class TestDataPipeline(unittest.TestCase):

    def test_extract_dependencies(self):
        print("\n--- [TEST 1] Regex Dependensi ---")
        # 1. Buat file dummy
        dummy_filename = os.path.join(CURRENT_DIR, "test_dummy.sql")
        content = """
        SELECT * FROM source.users
        JOIN `tmp.Orders` ON ...      
        LEFT JOIN final.summary ON ...
        """
        
        with open(dummy_filename, "w", encoding='utf-8') as f:
            f.write(content)
            
        try:
            #  Jalankan fungsi
            deps = extract_dependencies(dummy_filename)
            print(f"   Hasil deteksi: {deps}")
            
            # Validasi (Logic main.py memaksa lowercase)
            self.assertIn("tmp.orders", deps)
            self.assertIn("final.summary", deps)
            self.assertEqual(len(deps), 2)
            
        finally:
            # Bersih-bersih
            if os.path.exists(dummy_filename):
                os.remove(dummy_filename)

    def test_topological_sort(self):
        print("\n--- [TEST 2] Logika Pengurutan (Sequential) ---")
        # Skenario: Laporan -> Transaksi -> User
        mock_deps = {
            "laporan":   {"depends_on": ["transaksi"]},
            "transaksi": {"depends_on": ["user"]},
            "user":      {"depends_on": []}
        }
        
        urutan = get_sorted_execution_order(mock_deps)
        print(f"   Hasil urutan: {urutan}")
        
        # Validasi: User -> Transaksi -> Laporan
        self.assertEqual(urutan, ["user", "transaksi", "laporan"])

if __name__ == '__main__':
    unittest.main()