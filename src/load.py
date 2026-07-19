import shutil
import os

def load(final_df):
    # 1. Write to an internal Linux path first to avoid Windows/WSL2 Hadoop chmod errors
    tmp_path = "/tmp/gold_temp"
    final_path = "/opt/airflow/project/data/gold"
    
    final_df.write.mode("overwrite").parquet(tmp_path)
    
    # 2. Recreate target folder on Windows mount
    if os.path.exists(final_path):
        shutil.rmtree(final_path)
    os.makedirs(final_path, exist_ok=True)
    
    # 3. Copy file data ONLY (shutil.copyfile does not copy metadata/permissions, avoiding WSL2 error)
    for item in os.listdir(tmp_path):
        s = os.path.join(tmp_path, item)
        d = os.path.join(final_path, item)
        if os.path.isfile(s):
            shutil.copyfile(s, d)
            
    print("✅ Successfully copied Gold data to the mounted volume using copyfile!")