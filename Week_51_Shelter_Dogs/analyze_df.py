import polars as pl
import os

# Define Enum categories
enum_AGE = pl.Enum(['Adult', 'Baby', 'Senior', 'Young'])
enum_SEX = pl.Enum(['Male', 'Female', 'Unknown'])
enum_SIZE = pl.Enum(['Small', 'Medium', 'Large','Extra Large'])

root_file = 'allDogDescriptions'
if False: # os.path.exists(root_file + '.parquet'):
    print(f'{"*"*20} Reading {root_file}.parquet  {"*"*20}')
    df = pl.read_parquet(root_file + '.parquet')
    
else:
    print(f'{"*"*20} Reading {root_file}.csv  {"*"*20}')
    df = (
        pl.read_csv(root_file + '.csv', ignore_errors=True)
        .select(
            ID = pl.col('id').cast(pl.UInt32),
            ORG_ID = pl.col('org_id'),
            BREED_PRIMARY = pl.col('breed_primary'),
            BREED_MIXED = pl.col('breed_mixed'),
            AGE = pl.col('age').cast(enum_AGE),
            SEX = pl.col('sex').cast(enum_SEX),
            SIZE = pl.col('size').cast(enum_SIZE),
            FIXED = pl.col('fixed'),
            HOUSE_TRAINED = pl.col('house_trained'),
            SHOTS_CURRENT = pl.col('shots_current'),
            NAME = pl.col('name').str.strip_chars().str.to_titlecase(),
            DATE = pl.col('posted')
                .str.extract(r'(\d{4}-\d{2}-\d{2})', group_index=1)
                .str.to_date('%Y-%m-%d'),
            TIME = pl.col('posted')
                .str.extract(r'(\d{2}:\d{2}:\d{2})', group_index=1)
                .str.to_time('%H:%M:%S'),
            CONTACT_CITY = pl.col('contact_city'),
            CONTACT_STATE = pl.col('contact_state'),
            CONTACT_ZIP = pl.col('contact_zip').cast(pl.UInt32),
        )
        .filter(
            pl.col('CONTACT_STATE').str.contains(r'^[A-Z]{2}$')
        )
    )
    df.write_parquet(root_file + '.parquet')

# # Comprehensive analysis
print("\n" + "="*60)
print("DATAFRAME ANALYSIS")
print("="*60)

print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")

print("\n--- Data Types ---")
print(df.schema)

print("\n--- Missing Values ---")
print(df.null_count())

print("\n--- Basic Statistics ---")
print(df.describe())

print("\n--- Categorical Value Counts ---")
for col in ['AGE', 'SEX', 'SIZE']:
    print(f"\n{col}:")
    print(df[col].value_counts().sort('count', descending=True))

print("\n--- Sample Data ---")
print(df.head(3))
