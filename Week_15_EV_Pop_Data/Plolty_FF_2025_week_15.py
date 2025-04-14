import polars as pl
# pl.show_versions()

df = (
    pl.read_csv('Electric_Vehicle_Population_Data.csv')
    .select(
        VIN = pl.col('VIN (1-10)'),
        COUNTY = pl.col('County'),
        CITY = pl.col('City'),
        STATE = pl.col('State'),
        ZIP = pl.col('Postal Code'),
        YEAR = pl.col('Model Year'),
        MAKE = pl.col('Make'),
        MODEL = pl.col('Model'),
        EVTYPE = (
            pl.col('Electric Vehicle Type')
                .str.replace('Plug-in Hybrid Electric Vehicle (PHEV)', 'HYBRID', literal=True)
                .str.replace('Battery Electric Vehicle (BEV)', 'BATT', literal=True)
        ),
        RANGE = pl.col('Electric Range'),
        #  MSRP = pl.col('Base MSRP'), # MSRP are mostly 0, don't use
        LEG_DIST = pl.col('Legislative District'),
        # UTIL_CO = pl.col('Electric Utility'),  only specified for WA State, so exclude
        LOC = pl.col('Vehicle Location'),
    )
    .with_columns(
        LONG= (
            pl.col('LOC')
                .str.replace('POINT (', '', literal=True)
                .str.replace(')', '', literal=True) 
                .str.split(' ')
                .list.get(0)    # 1st element [0] is longitude (west-east)
                .cast(pl.Float64, strict=False)
        )
    )
    .with_columns(
        LAT = (
            pl.col('LOC')
                .str.replace('POINT (', '', literal=True) 
                .str.replace(')', '', literal=True) 
                .str.split(' ')
                .list.get(1)   # 2nd element [1] is latitude (north-south)
                .cast(pl.Float64)
        )
   )
   .drop('LOC')
)

# df.write_excel(
#     'df_1.xlsx',
#     autofit=True,
#     freeze_panes=(1,0)
# )


print(df.shape)
print(df.sample(10).glimpse())


# df = (

#     pl.read_csv('henley_results_cleaned.csv')
#     .drop('time', 'fawley_loser_leading')   # EDA shows 80% of time values are null, so drop this col
# )
#-----  EXPLORATRY DATA ANALYSIS ----------------------------------------------
# if False:
#     df_row_count = df.height
#     df_null = (df
#         #.fill_null(0)
#         .null_count()
#         .transpose(include_header=True)
#         .with_columns(
#             PCT_NUL = (
#             100*pl.col('column_0')/df_row_count
#             )
#         )
#         .sort('column_0', descending=True)
#     )
    # print(df_null)
    # # print(df.select('PCT_NULL', 'column_0'))
    # null_time_count = df.filter(pl.col('time').is_null()).height
    # not_null_time_count = df.filter(pl.col('time').is_not_null()).height

#     print(1/0)



#     pct_null_time = 100 * null_time_count / (null_time_count + not_null_time_count)
#     print(f'{null_time_count = }')
#     print(f'{not_null_time_count = }')
#     print(f'{pct_null_time = }')


# for col in df.columns:
#     print(df[col].value_counts())
# print(df.columns)
# print(df.glimpse())