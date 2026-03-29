import sys
import akshare as ak
import pandas as pd
from pathlib import Path

print(sys.executable)
print(sys.version)
print("akshare ok")
print("pandas ok")

out_path = Path(r"W:\AshareScanner\output\test_write.txt")
out_path.write_text("write test ok", encoding="utf-8")
print(f"wrote: {out_path}")