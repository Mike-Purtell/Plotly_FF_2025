import polars as pl
import plotly.express as px

# ----- READ AND CLEAN DATA ----------------------------------------------------
df = (
    pl.scan_csv('us-hurricanes.csv', ignore_errors=True)
    .select(
        DECADE = (
            pl.col('year')
            .cast(pl.String)
            .str.slice(0,3) + pl.lit('0s')
        ),
        CATEGORY = pl.col('category').cast(pl.String),      
        # NAME = pl.col('name')
    )
    .filter(pl.col('CATEGORY').is_in([str(i+1) for i in range(5)]))
    .with_columns(
          COUNT = (
              pl.col('DECADE')
                .count()
                .over('DECADE','CATEGORY')
          )
    )
    
    .unique(['DECADE', 'CATEGORY'])
    .sort('DECADE', 'CATEGORY')
    .collect()
    .pivot(
        on='CATEGORY',
        index='DECADE'
    )
    .with_columns(pl.col([str(i+1) for i in range(5)]).fill_null(0))
    .with_columns(
        WEIGHTED_STRENGTH = (
            (pl.col('1') * 1) +
            (pl.col('2') * 2) +
            (pl.col('3') * 3) +
            (pl.col('4') * 4) +
            (pl.col('5') * 5)
        )
    )
)

# ----- BAR CHART OF HURRICANE STRENGTH BY DECADE ------------------------------
fig=px.bar(
    df,
    'DECADE',
    'WEIGHTED_STRENGTH',
    template='simple_white',
    title=(
        'HURRICANE WEIGHTED STRENGTH BY DECADE<br>' +
        '<sup>' + 
            'Weighted Strength = ' +
            'n(cat1)*1 + n(cat2)*2 + n(cat3)*3 + n(cat4)*5 + n(cat5)*5<br>'
            'DATA_SOURCE: NOAA HurricaneReserach Division' +
        '</sup>'
    ),
    height=500, width=800
)
# ---- ADD HORIZONTAL LINE AT MEAN VALUE, WITH LABEL ---------------------------
fig.add_hline(
    y=df['WEIGHTED_STRENGTH'].mean(),
    line_width=3, line_dash='dash', 
    line_color='green',
    annotation_text='Mean', 
)
fig.show()