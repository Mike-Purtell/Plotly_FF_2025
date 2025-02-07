import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

fig_height = 600
fig_width = 600
dollars_per_pound = 1.24
data = 'https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/'
data += 'main/2025/week-5/Steam%20Top%20100%20Played%20Games%20-%20List.csv'

#-------------------------------------------------------------------------------
#  FUNCTIONS FOR ANNOTATION AND UPDATE LAYOUT ARE SIMILAR FOR EACH PLOT
#-------------------------------------------------------------------------------
def annotate_disclaimer(fig):
    fig.add_annotation(
        text='Data changes frequently, visit Data Source (Steam) for updates',
        showarrow=False,
        x=0.1, xref='paper',
        y=1.05, yref='paper',
        xanchor='left'
    )
    return fig

def update_my_layout(fig, title):
    fig.update_layout(
        legend=dict(
            #y=0.5,
            orientation='h'
        ),
        legend_title_text=title,
        showlegend=True,
        xaxis_title='',
        yaxis_title='NUMBER OF PLAYERS'
    )
    return fig

#-------------------------------------------------------------------------------
#   READ AND CLEAN DATA, TO DATAFRAME df
#-------------------------------------------------------------------------------
df = (
    pl.read_csv(data)
    .with_columns(
        pl.col('Current Players', 'Peak Today')
            .str.replace_all(',', '')
            .cast(pl.UInt32())
    )
    .with_columns(
        pl.col('Price')
        .str.replace('Free To Play', '0.0')
        .str.replace('£', '')
        .str.replace(',', '')
        .cast(pl.Float32)
    )
    .with_columns(
        BUZZ_PCT = 
            100.0 * 
            (pl.col('Peak Today') / pl.col('Current Players') - 1.0)
    )
    .with_columns(
        Name = pl.col('Name') + pl.lit(' (') +
            pl.col('BUZZ_PCT').round(1).cast(pl.String) + pl.lit('%)')
        )
    .sort('Current Players', descending=True)
    .select(pl.col('Rank', 'Name', 'Current Players','Peak Today','Price', 'BUZZ_PCT'))
)

#-------------------------------------------------------------------------------
#   FILTER MAIN DATAFRAME df TO SELECT PAID GAMES
#-------------------------------------------------------------------------------
df_paid = (
    df
    .filter(pl.col('Price') > 0.0)
    .sort('BUZZ_PCT', descending=True)  # pre-sorted, used just in case
    # redo the Ranks for paid games only, using "with_row_index"
    .with_row_index(offset=1)
    .drop('Rank')
    .rename({'index': 'Rank', 'Name':'PAID_GAME'})
    .head(10)
    .transpose(include_header = True, column_names = 'PAID_GAME')
    .rename({'column' : 'PAID_GAME'})
    .filter(pl.col('PAID_GAME').is_in(['Current Players','Peak Today']))
)
#-------------------------------------------------------------------------------
#   FILTER MAIN DATAFRAME df TO SELECT FREE GAMES
#-------------------------------------------------------------------------------
df_free = (
    df.filter(pl.col('Price') == 0.0)
    .sort('BUZZ_PCT', descending=True)  # pre-sorted, used just in case
    # redo the Ranks for free games only, using "with_row_index"
    .with_row_index(offset=1)
    .drop('Rank')
    .rename({'index': 'Rank', 'Name':'FREE_GAME'})
    .select(pl.col('FREE_GAME', 'Current Players', 'Peak Today'))
    .head(10)
    .transpose(include_header = True, column_names = 'FREE_GAME')
    .rename({'column' : 'FREE_GAME'})
    .filter(pl.col('FREE_GAME').is_in(['Current Players','Peak Today']))
)
#-------------------------------------------------------------------------------
#   SUP TITLE IS SAME FOR BOTH GRAPHS
#-------------------------------------------------------------------------------
sup_title = (
    '<sup>Data Source: ' +
    '<a href="https://store.steampowered.com/charts/mostplayed">' +
    'Steam</a></sup>'
)

#-------------------------------------------------------------------------------
#   fig_paid is a 2-point line for each selected paid game 
#-------------------------------------------------------------------------------
fig_paid=px.scatter(
    df_paid,
    'PAID_GAME',
    [c for c in df_paid.columns if c != 'PAID_GAME'],
    template='simple_white',
    height=fig_height, width=fig_width,
    title = 'PAID GAMES WITH HIGHEST BUZZ - TOP 10<br>' + sup_title
)
fig_paid.update_traces(mode ='lines+markers')
fig_paid = update_my_layout(fig_paid, 'PAID GAME BUZZ (%)')
fig_paid = annotate_disclaimer(fig_paid)
fig_paid.write_html('fig_paid.html')
fig_paid.show()

#-------------------------------------------------------------------------------
#   fig_free is a 2-point line for each selected free game 
#-------------------------------------------------------------------------------
fig_free=px.scatter(
    df_free,
    'FREE_GAME',
    [c for c in df_free.columns if c != 'FREE_GAME'],
    template='simple_white',
    height=fig_height, width=fig_width,
    title = 'FREE GAMES WITH HIGHEST BUZZ - TOP 10<br>' + sup_title
)

fig_free.update_traces(mode ='lines+markers')
fig_free = update_my_layout(fig_free, 'FREE GAME BUZZ(%)')
fig_free = annotate_disclaimer(fig_free)
fig_free.write_html('fig_free.html')
fig_free.show()
