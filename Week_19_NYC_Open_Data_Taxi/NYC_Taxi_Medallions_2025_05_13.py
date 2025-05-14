import polars as pl
pl.Config().set_tbl_cols(15)
import polars.selectors as cs
import plotly.express as px
# exclude data older than 2024
# look at non-approved applications with 4 of 5 requirements completed. For 
# applicants that came close needing only 1 more requirment, what was it?
 
eval_cols = [
    'DRUG_TEST','WAV_COURSE','DEFENSIVE_DRIVING', 'DRIVER_EXAM',
    'MEDICAL_CLEARANCE'
]

df = (
    pl
    .scan_csv('TLC_New_Driver_Application_Status_20250512.csv')
    .select(
        APP_DATE = pl.col('App Date').str.to_date(format='%m/%d/%Y'),
        STATUS = pl.col('Status')
            .str.replace('Approved - License Issued', 'Approved')
            .str.replace('Pending Fitness Interview', 'Fitness_Interview'),
        DRUG_TEST = pl.when(pl.col('Drug Test') == 'Complete')
                       .then(pl.lit(1))
                       .otherwise(pl.lit(0)),
        WAV_COURSE = pl.when(pl.col('WAV Course') == 'Complete')
                .then(pl.lit(1))
                .otherwise(pl.lit(0)),
        DEFENSIVE_DRIVING = pl.when(pl.col('Defensive Driving') == 'Complete')
                .then(pl.lit(1))
                .otherwise(pl.lit(0)),
        DRIVER_EXAM = pl.when(pl.col('Driver Exam') == 'Complete')
                .then(pl.lit(1))
                .otherwise(pl.lit(0)),
        MEDICAL_CLEARANCE = pl.when(pl.col('Medical Clearance Form') == 'Complete')
                .then(pl.lit(1))
                .otherwise(pl.lit(0)),
    )
    .filter(pl.col('APP_DATE') >= pl.datetime(2024,1,1)) # exclude pre-2024
     # count occurances of 'Complete in 5 listed columns
    .with_columns(COMPLETES = pl.sum_horizontal(eval_cols))
    .select(
        DRUG_TEST=pl.col('DRUG_TEST')
            .sum()
            .over('COMPLETES'),
        WAV_COURSE=pl.col('WAV_COURSE')
            .sum()
            .over('COMPLETES'),
        DEFENSIVE_DRIVING=pl.col('DEFENSIVE_DRIVING')
            .sum()
            .over('COMPLETES'),
        DRIVER_EXAM=pl.col('DRIVER_EXAM')
            .sum()
            .over('COMPLETES'),
        MEDICAL_CLEARANCE=pl.col('MEDICAL_CLEARANCE')
            .sum()
            .over('COMPLETES'),
        COMPLETES = pl.sum_horizontal(eval_cols)     
    )
    .with_columns(
        LEN=pl.col('DRUG_TEST')
            .count()
            .over('COMPLETES'),         
    )
    .with_columns(pl.col('COMPLETES').cast(pl.String))
    # .sort('APP_DATE')
    .unique('COMPLETES')
    .sort('COMPLETES')
    .collect()
    # .transpose(include_header=True, header_name='COMPLETES', column_names='COMPLETES')
    #.sort
)
dict_complete_len = dict(zip(df['COMPLETES'], df['LEN']))
print(dict_complete_len)
print(df)
df = (
    df
    .transpose(
        include_header=True, 
        column_names='COMPLETES',
        header_name='REQUIREMENT', 
    )
    .filter(pl.col('REQUIREMENT') != 'LEN')
    .sort('REQUIREMENT')
)
print(df)

requirement_num = df.height
print(f'{requirement_num =}')
for col in [str(i) for i in range(6)]:
    print(f'{col = }')
    data_len = dict_complete_len.get(col)
    print(f'{data_len = }')
    df_bar = (
        df
        .select(pl.col('REQUIREMENT',col))
        .rename({col:'COMPLETE'})
        .with_columns(
            INCOMPLETE = data_len - pl.col('COMPLETE')
        )
    )

    print(df_bar)
    fig = px.bar(
        df_bar, 
        y='REQUIREMENT', 
        x=['COMPLETE', 'INCOMPLETE'], 
        # x=col,
        height=400, width=600, 
        template='simple_white',
        title=f'COMPLETED {col} of {requirement_num} REQUIREMENTS'
    )
    fig.update_layout(yaxis=dict(autorange="reversed"))
    fig.show()
