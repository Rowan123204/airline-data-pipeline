def load(final_df):  # <- Takes the DataFrame returned by transform
    final_df.write.mode("overwrite").parquet("../data/gold")