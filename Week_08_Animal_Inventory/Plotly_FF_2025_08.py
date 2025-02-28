import polars as pl
import plotly.express as px
df = (
    pl.scan_csv('Dallas_Animal_Shelter_Data_Fiscal_Year_Jan_2024.csv')
    .filter(pl.col('Animal_Type').str.to_uppercase() == 'DOG')
    .select(
        BREED = pl.col('Animal_Breed').str.to_titlecase(),
        IN_TYPE = pl.col('Intake_Type').str.to_titlecase().cast(pl.Categorical),
        OUT_TYPE = pl.col('Outcome_Type').str.to_titlecase(),
        IN_DATE = pl.col('Intake_Date').str.to_date('%d/%m/%Y', strict=False),
        OUT_DATE = pl.col('Outcome_Date').str.to_date('%d/%m/%Y', strict=False),
    )
    .filter(pl.col('IN_TYPE') == 'Stray')
    .with_columns(
        OUTCOME = 
            pl.when(pl.col('OUT_TYPE').is_in(
                ['Returned To Owner', 'Adoption']))
               .then(pl.lit('POS')).otherwise(pl.lit('NEG'))
    )
    .with_columns(
        BREED_OUTCOME_COUNT = 
            pl.col('BREED').count().over('BREED', 'OUTCOME')
    )
    .with_columns(
        TOTAL_DAYS = (
            pl.col('OUT_DATE') 
            - 
            pl.col('IN_DATE')
        )
        .dt.total_days()
        .cast(pl.Int16)
    )
    # .filter(pl.col('TOTAL_DAYS') > 1)
    .unique(['BREED', 'OUTCOME'])
    .collect()
    .pivot(
        on = 'OUTCOME',
        index='BREED',
        values='BREED_OUTCOME_COUNT'
    )
    .drop_nulls()
    .with_columns(
        POS_PCT = 
        100* pl.col('POS')
        /
        (
            pl.col('POS') + pl.col('NEG')
        ))
    .with_columns(
        TOTAL_DAYS = (
            pl.col('POS') 
            + 
            pl.col('NEG')
        )
    )
    .with_columns(NEG_PCT = (100 - pl.col('POS_PCT')))
    .filter(pl.col('BREED') != 'Mixed Breed')
    .sort('POS_PCT', descending=False)
    .select('BREED', 'TOTAL_DAYS', 'POS', 'NEG', 'POS_PCT', 'NEG_PCT')
)
fig = px.bar(
    df,
    x=['POS_PCT', 'NEG_PCT'],
    y='BREED',
    height=1000, width=1200,
    template='simple_white',
    title = (
        'SHELTER STRAY DOGS: PAWS. vs NEG. OUTCOMES<br>' + 
        '<sup>POS OUTCOME MEANS OWNER FOUND OR ADOPTION<sup>'
    )
)
fig.update_layout(
    xaxis_title='Percentage'.upper(),
    legend_title='OUTCOME(%)')
fig.show()