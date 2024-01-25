#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import gzip
import shutil
from multiprocessing import Pool
import re
import json
from time import time
import importlib
from random import shuffle
import math

import polars as pl


# utils is our local utility module
# if we change utils.py, and re-run a normal 'import'
# python won't reload it by default. (Since it's already loaded.)
# So we force a reload
import utils
importlib.reload(utils)


# In[2]:


disk_one = '/media/matthew/Tux/AppliedEconometrics/data'
disk_two = '/media/matthew/nemweb/AppliedEconometrics/data'
repo_data_dir = '/home/matthew/Documents/TSE/AppliedEconometrics/repo/data/'
laptop_data_dir = '/home/matthew/data/'

all_csv_gz_dir = os.path.join(laptop_data_dir, '01-E-split-mapped-csv')
all_csv_gz_archive = os.path.join(laptop_data_dir, '01-E-split-mapped-csv-done')
all_csv_dir = os.path.join(laptop_data_dir, '01-F-gunzipped-csvss')
all_parquet_batch_dir = os.path.join(laptop_data_dir, '01-F-parquet-batches')
#dest_dir = os.path.join(laptop_data_dir, '01-F-parquet-polars')

schema_path = os.path.join(repo_data_dir, 'schemas.json')


num_processes = os.cpu_count() - 2 # *2 because we assume hyperthreading and want one spare to keep doing other stuff


# In[3]:


logger = utils.Logger(os.path.join(repo_data_dir, 'logs.txt'), flush=True)
logger.info("Initialising Logger")


# In[4]:


utils.renice()


# In[5]:


version_col_name = 'SCHEMA_VERSION'
top_timestamp_col_name = 'TOP_TIMESTAMP'


# In[6]:


with open(schema_path, 'r') as f:
    schemas = json.load(f)


# In[7]:


# AEMO's schemas have Oracle SQL types
# map those to types polars can use
# if date_as_str, return string instead of datetime
# (because pyarrow can't read datetimes when parsing from CSV)
def aemo_type_to_polars_type(t: str, tz=None, date_as_str=False):
    t = t.upper()
    if re.match(r"VARCHAR(2)?\(\d+\)", t):
        return pl.String()
    if re.match(r"CHAR\((\d+)\)", t):
        # single character
        # arrow has no dedicated type for that
        # so use string
        # (could use categorical?)
        return pl.String()
    elif t.startswith("NUMBER"):
        match = re.match(r"NUMBER ?\((\d+), ?(\d+)\)", t)
        if match:
            whole_digits = int(match.group(1))
            decimal_digits = int(match.group(2))
        else:
            # e.g. NUMBER(2)
            match = re.match(r"NUMBER ?\((\d+)", t)
            assert match, f"Unsure how to cast {t} to arrow type"
            whole_digits = int(match.group(1))
            decimal_digits = 0
            
        if decimal_digits == 0:
            # integer
            # we assume signed (can't tell unsigned from the schema)
            # but how many bits?
            max_val = 10**whole_digits

            if 2**(8-1) > max_val:
                return pl.Int8()
            elif 2**(16-1) > max_val:
                return pl.Int16()
            elif 2**(32-1) > max_val:
                return pl.Int32()
            else:
                return pl.Int64()
        else:
            # we could use pa.decimal128(whole_digits, decimal_digits)
            # but we don't need that much accuracy
            return pl.Float64()
    elif (t == 'DATE') or re.match(r"TIMESTAMP\((\d)\)", t):
        # watch out, when AEMO say "date" they mean "datetime"
        # for both dates and datetimes they say "date",
        # but both have a time component. (For actual dates, it's always midnight.)
        # and some dates go out as far as 9999-12-31 23:59:59.999
        # (and some dates are 9999-12-31 23:59:59.997)
        if date_as_str:
            return pl.String()
        else:
            return pl.Datetime(time_unit='ms', time_zone=tz)
    else:
        raise ValueError(f"Unsure how to convert AEMO type {t} to polars type")


# In[8]:


# for the largest tables, far larger than the smallest 200 combined,
# let's drop the columns we know we won't use
# to make it faster
def get_cols_to_drop(table):
    if table == 'DISPATCHLOAD':
        to_drop = []
        cols = list(schemas[table]['columns'].keys())
        for c in cols:
            if any(c.upper().startswith(prefix) for prefix in ['RAISE', 'LOWER', 'VIOLATION', 'MARGINAL']) \
               and c not in schemas[table]['primary_keys']:
                to_drop.append(c)
        logger.info(f"Will drop {to_drop} from {table}")
        return to_drop
    else:
        return []


# In[11]:


def gunzip_file(source_path, dest_path):
    assert source_path.lower().endswith('.csv.gz')
    
    # create a destination folder
    utils.create_dir(file=dest_path)
    
    try:
        with gzip.open(source_path, 'r') as f_in:
            with open(dest_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except Exception:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        raise

# only convert this many CSV files into parquet at once
# see my issue here:
# https://github.com/pola-rs/polars/issues
batch_size = 1000

# list files in a directory, recursively
# returning an iterable of one full path at a time
# if randomised=True, shuffle the list of files
def walk(dir, randomised=False):
    if randomised:
        # need to physicalise into a list
        # and mutate
        paths = list(walk(dir, randomised=False))
        assert isinstance(paths, list)
        shuffle(paths)
        # don't mix yield and return in one function
        for p in paths:
            yield p
    else:
        for (dir,subdirs,files) in os.walk(dir):
            for file in files:
                yield os.path.join(dir, file)


def csv_gz_to_parquet(table):
    source_schema = {c: aemo_type_to_polars_type(t, date_as_str=True, tz=None) for (c,t) in schemas[table]['columns'].items()}
    dest_schema = {c: aemo_type_to_polars_type(t, date_as_str=False, tz='Australia/Brisbane') for (c,t) in schemas[table]['columns'].items()}

    table_csv_gz_dir = os.path.join(all_csv_gz_dir, table)
    table_csv_gz_archive = os.path.join(all_csv_gz_archive, table)
    table_csv_dir = os.path.join(all_csv_dir, table)
    table_parquet_batched_dir = os.path.join(all_parquet_batch_dir, table)

    cols_to_drop = get_cols_to_drop(table)
    
    logger.info(f"Converting {table} from .csv.gz in {table_csv_gz_dir} to .csv in{table_csv_dir} then to .parquet in {table_parquet_batched_dir}")

    csv_gz_paths_unbatched = []
    csvgz_size = 0
    for (dir,subdirs,files) in os.walk(table_csv_gz_dir):
            for file in files:
                path = os.path.join(dir, file)
                csv_gz_paths_unbatched.append(path)
                csvgz_size = csvgz_size + os.path.getsize(path)

    # make batch sizes (by bytes) more consistent
    shuffle(csv_gz_paths_unbatched)

    # calculate batch size
    # min of:
    #  max_batch_size
    #  whatever results in uncompressed CSVs taking up too much space
    
    # emperically, this is the effectiveness of our gzip algorithm
    compression_ratio = 30
    
    # emperically, polars can't handle more than this many files at once
    # (approximate)
    # https://github.com/pola-rs/polars/issues
    # 10000 is the limit normally
    # but for the largest tables (e.g. DISPATCHLOAD) I'm still getting freezing
    max_batch_size = 20

    free_bytes = shutil.disk_usage(all_csv_dir).free
    free_bytes = free_bytes / 2 # leave room for CSV + parquet + extra
    
    # figure out total uncompressed size
    uncompressed_size = csvgz_size * compression_ratio

    # divide to get number of batches
    num_batches = uncompressed_size / free_bytes

    batch_size = math.ceil(len(csv_gz_paths_unbatched) / num_batches)
    batch_size = min(batch_size, max_batch_size)
    num_batches = math.ceil(len(csv_gz_paths_unbatched) / batch_size)

    logger.info(f"For table {table}, {len(csv_gz_paths_unbatched)} .csv.gz files totaling {csvgz_size} bytes, estimating {uncompressed_size=}, {num_batches=}, so choosing {batch_size=}")

    # remove files from previous run
    shutil.rmtree(table_csv_dir, ignore_errors=True)
    shutil.rmtree(table_parquet_batched_dir, ignore_errors=True)
    utils.create_dir(table_parquet_batched_dir)
    
    for (batch_num, csv_gz_paths) in enumerate(utils.batched(csv_gz_paths_unbatched, batch_size), start=1):
        logger.info(f"Unzipping {len(csv_gz_paths)} CSV.gz in batch {batch_num}/{num_batches} for {table}\n")

        assert all(p.lower().endswith('.csv.gz') for p in csv_gz_paths), f"Found a file in {table_csv_gz_dir} which does not end in .csv.gz"
        csv_paths = [os.path.join(table_csv_dir, os.path.relpath(path=csv_gz_path, start=table_csv_gz_dir)).replace('.gz', '') for csv_gz_path in csv_gz_paths]

        with Pool() as p:
            p.starmap(gunzip_file, zip(csv_gz_paths, csv_paths))

        logger.info(f"Scanning {len(csv_gz_paths)} CSVs in batch {batch_num}/{num_batches} for {table}\n")
        datasets = []
        for (csv_gz_path, csv_path) in zip(csv_gz_paths, csv_paths):
            
            match = re.search(f"/{version_col_name}=(\d+)/", csv_path)
            assert match, f"Unable to extract schema version from {csv_path}"
            schema_version = int(match.group(1))
    
            match = re.search(f"/{top_timestamp_col_name}=([\d_]+)/", csv_path)
            assert match, f"Unable to extract top_timestamp from {csv_path}"
            top_timestamp = match.group(1)

            gunzip_file(source_path=csv_gz_path, dest_path=csv_path)
    
            ds = pl.scan_csv(csv_path, dtypes=source_schema, try_parse_dates=False, low_memory=True)
            ds = ds.with_columns(pl.lit(schema_version, dtype=pl.UInt8()).alias(version_col_name))
            ds = ds.with_columns(pl.lit(top_timestamp, dtype=pl.String()).alias(top_timestamp_col_name))
                
            datasets.append(ds)
        logger.info(f"Scanned {len(csv_gz_paths)} CSVs in batch {batch_num}/{num_batches} for {table}\n")
    
        assert len(datasets) > 0, f"No CSV files found for {table}?"
    
        ds = pl.concat(datasets, how='diagonal')

        
        # drop columns we don't need (in the biggest tables)
        for col in cols_to_drop:
            if col in ds.columns:
                ds = ds.select(pl.exclude(col))
    
        # parse datetimes into actual datetime
        for (c,t) in dest_schema.items():
            if isinstance(t, pl.Datetime):
                ds = ds.with_columns(pl.col(c).str.strptime(pl.Datetime, "%Y/%m/%d %H:%M:%S", strict=False).dt.replace_time_zone(time_zone="Australia/Brisbane"))
        
        pq_path = os.path.join(table_parquet_batched_dir, f"{batch_num:04}.parquet")
    
        logger.info(f"Beginning processing of {len(datasets)} CSVs in batch {batch_num} for {table}\n")
        try:
            ds.sink_parquet(pq_path)
        except pl.ComputeError as ex:
            # tidy up. Don't leave a corrupt file there
            if os.path.exists(pq_path):
                os.remove(pq_path)
                
            # sometimes AEMO data has a float when we expect an int
            logger.exception(ex)
            match = re.search(r"could not parse `(\d+)\.(\d+)` as dtype `[iu]\d+` at column '([_\w]+)'", str(ex))
            if match:
                print(ex)
                whole_digits = int(match.group(1))
                decimal_digits = int(match.group(2))
                col = match.group(3)
                logger.warn(f"Changing column {col} in {table} from {schemas[table]['columns'][col]} to float")
                schemas[table]['columns'][col] = f"NUMBER({whole_digits},{decimal_digits})"
                return csv_gz_to_parquet(table)
            else:
                raise
        else:
            # validate that what we just wrote was not corrupt
            pl.scan_parquet(pq_path).fetch()
            shutil.rmtree(table_csv_dir)

    
    # move the source .csv.gz files to another directory
    # so if we fail on the nth table and retry
    # we don't reprocess this table
    utils.create_dir(table_csv_gz_archive)
    os.rename(table_csv_gz_dir, table_csv_gz_archive)
    
    logger.info(f"Finished converting table {table} from .csv.gz to .parquet")

def delete_temp_csv(table):
    dir = os.path.join(csv_dir, table)
    logger.info(f"Deleting intermediate ungzipped CSVs in {dir} to save space")
    shutil.rmtree(dir)
    
def delete_temp_parquet(table):
    dir = os.path.join(all_parquet_batch_dir, table)
    logger.info(f"Deleting intermediate, duplicated parquets in {dir} to save space")
    shutil.rmtree(dir)


# In[ ]:


logger.reset()
tables = os.listdir(all_csv_gz_dir)
shuffle(tables)
for table in tables:
    #if table not in ['BIDOFFERPERIOD', 'PERDEMAND']:
    csv_gz_to_parquet(table)
    
    #concat_and_dedup(table)
    
    #delete_temp_parquet(table)
