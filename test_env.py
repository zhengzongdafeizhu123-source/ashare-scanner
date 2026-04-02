import sys
import akshare as ak
import pandas as pd
from pathlib import Path

from project_paths import OUTPUT_ROOT

print(sys.executable)
print(sys.version)
print("akshare ok")
print("pandas ok")

out_path = OUTPUT_ROOT / "test_write.txt"
out_path.write_text("write test ok", encoding="utf-8")
print(f"wrote: {out_path}")
