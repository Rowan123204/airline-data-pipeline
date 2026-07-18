
from extract import extract
from transform import transform
from load import load

flights, airports, airlines = extract()
final_df = transform(flights, airports, airlines)
load(final_df)
