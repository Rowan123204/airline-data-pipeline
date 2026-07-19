import shutil
import os

def load(final_df):
    # 1. Write to an internal Linux path first to avoid Windows/WSL2 Hadoop chmod errors
    tmp_path = "/tmp/gold_temp"
    final_path = "/opt/airflow/project/data/gold"
    
    final_df.write.mode("overwrite").parquet(tmp_path)
    
    # 2. Use standard Python to move the files to the Windows-mounted volume
    if os.path.exists(final_path):
        shutil.rmtree(final_path)
    shutil.copytree(tmp_path, final_path)
    print("✅ Successfully copied Gold data to the mounted volume!")