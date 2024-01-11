library(arrow)
library(microbenchmark)

path <- 'data/01-F-one-parquet-per-table/DISPATCHLOAD.parquet'

df <- read_parquet(path)

key_columns <- c('DUID',
                 'INTERVENTION',
                 'RUNNO',
                 'SETTLEMENTDATE',
)
sort_columns <- c('SCHEMA_VERSION', 'LASTCHANGED')


# Test the performance of arrange + distinct
benchmark_arrange_distinct <- microbenchmark(
  df_arrange_distinct = {
    df %>%
      arrange(a, b, desc(c), desc(d)) %>%
      distinct(a, b, .keep_all = TRUE)
  }
)

# Test the performance of group_by + filter
benchmark_group_by_filter <- microbenchmark(
  df_group_by_filter = {
    df %>%
      group_by(a, b) %>%
      arrange(desc(c), desc(d)) %>%
      filter(row_number() == 1)
  }
)

# Print the results
print(benchmark_arrange_distinct)
print(benchmark_group_by_filter)

# read the parquet file associated with the given table
# if columns is specified (a list of strings) only load those columns
# if primary_key is specified (list of strings), deduplicate based on that
# (choosing the latest LASTCHANGED column to do so)
# otherwise deduplicate based on all columns
load_data <- function(table, columns=NULL, primary_keys=NULL){
  source_dir <- file.path('data', '06-one-parquet-per-table')  
  path <- file.path(source_dir, paste(table, 'parquet', sep = "."))
  if (missing(columns)){
    df <- read_parquet(path)
  } else {
    if (!missing(primary_keys) && !('LASTCHANGED' %in% columns)){
      columns <- append(columns, 'LASTCHANGED')
    }
    df <- read_parquet(path, col_select=columns)
  }
  # deduplicate based on all columns
  #df <- df |> distinct()
  if (!missing(primary_keys)){
    # deduplicate based on primary_keys columns
    # choosing the one with highest LASTCHANGED
    
    # assert LASTCHANGED is present
    #df |> pull(LASTCHANGED)
    
    df <- df |>
      group_by(across(all_of(primary_keys))) |>
      arrange(desc(LASTCHANGED)) |>
      slice(1) |>
      ungroup()
  }
  return(df)
}
dispatchload <- load_data('DISPATCHLOAD')
dispatchload <- load_data('DISPATCHLOAD',
                          columns = c("SETTLEMENTDATE",
                                      "DUID",
                                      "RUNNO",
                                      "INITIALMW",
                                      "INTERVENTION"),
                          primary_keys=c(
                            "DUID",
                            "INTERVENTION",
                            "RUNNO",
                            "SETTLEMENTDATE"
                          ))
