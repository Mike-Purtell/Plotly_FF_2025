import polars as pl
# pl.show_versions()

df = (
    pl.read_csv('henley_results_cleaned.csv')
    .drop('time', 'fawley_loser_leading')   # EDA shows 80% of time values are null, so drop this col
)

#-----  EXPLORATORY DATA ANALYSIS ----------------------------------------------
if False:
    df_row_count = df.height
    df_null = (df
        #.fill_null(0)
        .null_count()
        .transpose(include_header=True)
        .with_columns(
            PCT_NUL = (
            100*pl.col('column_0')/df_row_count
            )
        )
        .sort('column_0', descending=True)
    )
    print(df_null)
    # print(df.select('PCT_NULL', 'column_0'))
    null_time_count = df.filter(pl.col('time').is_null()).height
    not_null_time_count = df.filter(pl.col('time').is_not_null()).height
    pct_null_time = 100 * null_time_count / (null_time_count + not_null_time_count)
    print(f'{null_time_count = }')
    print(f'{not_null_time_count = }')
    print(f'{pct_null_time = }')
for col in df.columns:
    print(df[col].value_counts())
print(df.columns)
print(df.glimpse())