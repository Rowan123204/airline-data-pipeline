def load(final_df):  # ← بتاخد اللي رجعته transform
    final_df.write.mode("overwrite").parquet("gold_data")