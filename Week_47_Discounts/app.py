import polars as pl
import polars.selectors as cs
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

'''
Top-left (TL): pick one country, timeline showing total sales of selected country
          include horizontal bar to show mean values
          color green above mean, red below mean
Top-right(TR): pick on or more countries, cumulative sales on timeline
Bottom-left(BL): show total sales of country in Top-left, with group by market segment
Bottom-right(BR): choropleth to sales by country
 one segment, timeline showing total sales of selected s
'''

#----- GLOBALS -----------------------------------------------------------------
style_horizontal_thick_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '40px', 
            'fontFamily': 'Arial','font-weight': 'bold', 'color': 'gray'}

template_list = ['ggplot2', 'seaborn', 'simple_white', 'plotly','plotly_white',
    'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none']

dmc_text_red = {
    'fontSize':'16px', 
    'color':'red', 
    'textAlign':'left',
    'marginLeft':'100px'
}
# use dictionary dmc_text_red for and modify the color
dmc_text_gray = dmc_text_red.copy()
dmc_text_gray['color'] = 'gray'


{'fontSize':'16px', 'color':'gray', 'textAlign':'left','marginLeft':'100px'},

template_list = ['ggplot2', 'seaborn', 'simple_white', 'plotly','plotly_white',
    'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none']


#----- LOAD AND CLEAN THE DATASET ----------------------------------------------
df = (
    pl.read_csv('Sales.csv')
    .select(
        DATE = pl.col('Order Date')
            .str.split(' ')
            .list.first()
            .str.to_date(format='%m/%d/%Y'),
        PRODUCT = pl.col('Product ID'),
        SALES = pl.col('Sales').cast(pl.Float32),
        PROFIT = pl.col('Profit').cast(pl.Float32),
        COUNTRY = pl.col('Country').cast(pl.Categorical),
        SEGMENT = pl.col('Segment').cast(pl.Categorical),
    )
    .filter(pl.col('COUNTRY') != 'Bahrain') # Not enough data for Bahrain
)
print(df)

#----- CREATE GLOBAL LISTS------------------------------------------------------
countries = (sorted(df.unique('COUNTRY').get_column('COUNTRY').to_list()))
segments = (sorted(df.unique('SEGMENT').get_column('SEGMENT').to_list()))
print(f'{countries = }')
print(f'{segments = }')

#----- FUNCTIONS ---------------------------------------------------------------
def get_tl_country(country, template):
    df_country_timeline = ( # group by dynamic to bin by month
        df
        .filter(pl.col('COUNTRY') == country)
        .group_by_dynamic('DATE', every='1mo').agg(pl.col('SALES').sum())
    )
    print(df_country_timeline)
    mean_sales = df_country_timeline['SALES'].mean()
    df_country_timeline = (
        df_country_timeline
        .with_columns(MEAN_SALES = pl.lit(mean_sales))
        .with_columns(
            FILL_COLOR = pl.when(pl.col('SALES')>pl.col('MEAN_SALES'))
            .then(pl.lit('green'))
            .otherwise(pl.lit('red'))
        )   
    )
    print(df_country_timeline)
    print(f'{mean_sales = }')
    x = df_country_timeline['DATE'].to_numpy()
    y1 = df_country_timeline['SALES'].to_numpy()
    y2 = df_country_timeline['MEAN_SALES'].to_numpy()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=x, y=y1,
        mode='lines',
        line=dict(color='gray', width=1, shape='hv'),
        name='MONTHLY_SALES_TOT',
    ))

    fig.add_trace(go.Scatter(
        x=x, y=y2,
        mode='lines',
        line=dict(color='green', width=1, dash='dash'),
        name='MONTHLY_SALES_MEAN',
    ))

    fig.update_layout(
        template=template,
        hovermode='x unified',
        showlegend=False,
        title_text = f'{country} Sales Timeline',
        margin=dict(l=50, r=100, t=50, b=20),
    )

    fill_color = df_country_timeline['FILL_COLOR'].to_list()
    for i in range(len(x) - 1):
        fig.add_trace(go.Scatter(  # define trapezoidal area using 4 defined points
            x=[x[i], x[i+1], x[i+1], x[i]],
            y=[y1[i], y1[i], y2[i+1], y2[i+1]],
            fill='toself',
            fillcolor=fill_color[i],
            line=dict(width=0),
            mode='lines',
            hoverinfo='skip',
            showlegend=False,
            opacity=0.2
        ))
    fig.add_annotation(
        x=1, xref='paper',
        y = mean_sales, yref='y', 
        text = f'{country}<br>Mean<br>${mean_sales:,.0f}',
        xanchor='left', xshift=10, showarrow=False,
        font=dict(color='gray', size=12)
    )
    fig.update_xaxes(
        dtick='M6',
        tickformat='%b\n%Y',
        ticklabelmode="period",
        # ticks="inside",
        minor=dict(
            ticks="outside",
            tickwidth=2,
            ticklen=30,
            dtick="M12",

        ),
    )
    return fig

#----- DASH COMPONENTS------ ---------------------------------------------------
dmc_select_country = (
    dmc.Select(
        label='Pick one country',
        id='pick-country',
        data= countries,
        value=countries[7], # default is first country, alphabetic
        searchable=False,   # Enables search functionality
        clearable=True,     # Allows clearing the selection
        size='sm',
    ),
)

dmc_select_countries = (
    dmc.MultiSelect(
        label='Pick two or more countries',
        id='pick-countries',
        data= countries,
        value=[countries[0], countries[1]], # default is first country, alphabetic
        searchable=False,  # Enables search functionality
        clearable=True,    # Allows clearing the selection
        size='sm',
    ),
)

dmc_select_template = (
    dmc.Select(
        label='Pick your favorite Plotly template',
        id='template',
        data= template_list,
        value=template_list[2],
        searchable=False,  # Enables search functionality
        clearable=True,    # Allows clearing the selection
        size='sm',
    ),
)


#----- DASH APPLICATION --------------------------------------------------------
app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Sales data by Country', ta='center', style=style_h2),
    html.Hr(style=style_horizontal_thick_line), 
        dmc.Grid(children = [
        dmc.GridCol(dmc_select_template, span=3, offset = 1),
    ]),  
    dmc.Grid(children = [
        dmc.GridCol(dmc_select_country, span=3, offset = 1),
        dmc.GridCol(dmc_select_countries, span=3, offset = 4),
    ]),  
    dmc.Space(h=10),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='tl_country'), span=5, offset=1),  
        dmc.GridCol(dcc.Graph(id='tl_countries'), span=5, offset=1), 
    ]),
    dmc.Grid(children = [
        dmc.GridCol(dcc.Graph(id='tl_groupby'), span=5, offset=1), 
        dmc.GridCol(dcc.Graph(id='choro'), span=5, offset=1),           
    ]),
])
@app.callback(
    Output('tl_country', 'figure'),
    Input('pick-country', 'value'),
    Input('pick-countries', 'value'),
    Input('template', 'value')
)
def callback(country, countries, template):

    if not isinstance(countries, list):  # if value is not a list, make it one
        countries = [countries]  
    tl_country  = get_tl_country(country, template)
    # histogram = get_histogram(df, pick, template)
    # line_plot = get_line_plot(df, pick, template, aggregation)
    return tl_country
if __name__ == '__main__':
    app.run(debug=True)