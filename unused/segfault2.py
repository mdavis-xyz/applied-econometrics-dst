import os
import re
from random import choice
from itertools import islice

import polars as pl

# https://docs.python.org/3/library/itertools.html#itertools.batched
# added to standard library in 3.12
def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

source_dir = '/home/matthew/data/debug/segfault/data'
dest_dir = '/home/matthew/data/debug/segfault/'

schema = {
    'a': pl.String(),
    'b': pl.UInt16(),
    'c': pl.String(),
}

csv_data_ab = """a,b
A,123"""
csv_data_bc = """b,c
321,C"""

batch_size = 2
num_batches = 2
# generate the data
csv_paths = [os.path.join(source_dir, f"{i}.csv") for i  in range(num_batches * batch_size)]
for (i, csv_path) in enumerate(csv_paths):
    with open(csv_path, 'w') as f:
        f.write([csv_data_ab, csv_data_bc][i % 2])
print("Data generated")

datasets = []
for csv_path in csv_paths:
    ds = pl.scan_csv(csv_path, dtypes=schema)
    ds = ds.with_columns(pl.lit(543).alias('extra'))
    assert 'asd' not in ds.columns
    datasets.append(ds)

parquet_files = []
for (i, batch) in enumerate(batched(datasets, batch_size)):
    ds = pl.concat(batch, how='diagonal')
    temp_path = os.path.join(dest_dir, f"{i}.parquet")
    ds.sink_parquet(temp_path)
    print(f"{i}: {pl.read_parquet(temp_path)}")
    parquet_files.append(temp_path)
    
print(f"Merging {len(datasets)} parquets")
datasets = []
for pq_path in parquet_files:
    ds = pl.scan_parquet(pq_path) #.select(list(schema))
    datasets.append(ds)
    
dest_path = os.path.join(dest_dir, 'final.parquet')
pl.concat(datasets, how='diagonal').sink_parquet(dest_path)

print(pl.scan_parquet(dest_path).collect())

