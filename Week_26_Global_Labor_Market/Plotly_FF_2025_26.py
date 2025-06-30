import plotly.express as px
import polars as pl
import polars.selectors as cs
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

#----- GATHER DATA -------------------------------------------------------------
df_gender_parity_mgmt = (
    pl.scan_csv('gender-parity-in-managerial-positions.csv')
    .select(
       YEAR = pl.col('Year'),
       FEM_SHR_MGMT = pl.col('Female share in management'),
       FEB_SHR_EMP = pl.col('Female share in employment'),
       FEM_SHR_WRKN_POP = pl.col('Female share in the working-age population'),
    )
    .sort('YEAR')
    .collect()
)
df_gender_pay_gap = (
    pl.scan_csv('gender-pay-gap.csv')
    .select(
       YEAR = pl.col('Year'),
       WORLD = pl.col('World'),
       LOW_INCOME = pl.col('Low income'),
       LOWER_MID = pl.col('Lower-middle income'),
       UPPER_MID = pl.col('Upper-middle income'),
       HIGH_INCOME = pl.col('High income'),
    )
    .collect()
)

df_labor_productivity = (
    pl.scan_csv('labor-productivity.csv')
    .select(
       YEAR = pl.col('Year'),
       WORLD = pl.col('World'),
       AFRICA = pl.col('Africa'),
       AMERICAS = pl.col('Americas'),
       ARAB_STATES = pl.col('Arab States'),
       ASIA_PACIFIC = pl.col('Asia and the Pacific'),
       EUR_CENT_ASIA = pl.col('Europe and Central Asia'),
    )
    .collect()
)

df_unemployment = (
    pl.scan_csv('unemployment.csv')
    .select(
       YEAR = pl.col('Year'),
       WORLD = pl.col('World'),
       AFRICA = pl.col('Africa'),
       AMERICAS = pl.col('Americas'),
       ARAB_STATES = pl.col('Arab States'),
       ASIA_PACIFIC = pl.col('Asia and the Pacific'),
       EUR_CENT_ASIA = pl.col('Europe and Central Asia'),
    )
    .collect()
)

#----- GLOBALS -----------------------------------------------------------------
style_horiz_line = {'border': 'none', 'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,', 'fontsize': 32}

style_h2 = {'text-align': 'center', 'font-size': '32px', 
            'fontFamily': 'Arial','font-weight': 'bold'}

legend_font_size = 10
min_year = min([
    df_gender_parity_mgmt['YEAR'].min(),
    df_gender_pay_gap['YEAR'].min(),
    df_labor_productivity['YEAR'].min(),
    df_unemployment['YEAR'].min(),
])
max_year = max([
    df_gender_parity_mgmt['YEAR'].max(),
    df_gender_pay_gap['YEAR'].max(),
    df_labor_productivity['YEAR'].max(),
    df_unemployment['YEAR'].max()
])

dataset_names = [
    'Management Gender Parity',
    'Gender Pay Gap',
    'Labor Productivity',
    'Unemployment'
]

#----- FUNCTIONS----------------------------------------------------------------
def get_px_line(df, x_range, title, norm):
    df = (
        df
        .select(first:=cs.by_name('YEAR'), ~first)
        .filter(pl.col('YEAR') >= x_range[0])
        .filter(pl.col('YEAR') <= x_range[1])
    )
    df_cols = df.columns
    if norm:  # normalize the data, relative to the dataset's first year
        title = title + ' -- NORMALIZED'
        for col in df_cols[1:]:  # skips first column, which is the year
            df = (
                df
                .with_columns(
                    ((100*pl.col(col)/pl.col(col).first()-100)).alias(col)
                )
            )
        y_title='PERCENT CHANGE'
    else:
        title = title + ' -- RAW DATA'
        y_title='RAW DATA'
    fig=px.line(
        df,
        'YEAR',
        df_cols[1:],
        template='simple_white',
        line_shape='spline',
        title=title,
        markers=True
    )
    fig.update_traces(line=dict(width=0.5), hovertemplate=None)
    fig.update_layout(
        hoverlabel=dict(
            bgcolor="white",
            font_size=10,
            font_family='arial',
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.5,
            xanchor='center',
            x=0.5,
            font=dict(family='Arial', size=legend_font_size, color='black'),
            title=dict(
                text='',
                font=dict(family='Arial', size=legend_font_size, color='black'),
            )
        ),
        xaxis_title='',  # normally for showing units, but YEAR is obvious
        yaxis_title=y_title, 
        hovermode="x unified",
    )
    return fig

def get_df(dataset_name):
    if dataset_name == 'Management Gender Parity':
        return(df_gender_parity_mgmt)
    elif dataset_name == 'Gender Pay Gap':
        return(df_gender_pay_gap)
    elif dataset_name == 'Labor Productivity':
        return(df_labor_productivity)
    elif dataset_name == 'Unemployment':
        return(df_unemployment)
    else:
        print(f'NO VALID SELECION FOR {dataset_name}')
        return

def get_dataset_radio_picker():
    return(
    dmc.RadioGroup(
        children=dmc.Group(
            [dmc.Radio(item, value=item) for item in dataset_names]),
        id='dataset_radio',
        value=dataset_names[0],
        label='',
        ),
    )

def get_range_slider():
    range_year = max_year - min_year
    return(
        dmc.RangeSlider(
            id='range-slider-input',
            min=min_year,
            max=max_year,
            value=[min_year, max_year],
            step=1,
            marks=[
                {"value": min_year+(0.25*range_year), "label": "25%"},
                {"value": min_year+(0.5*range_year), "label": "50%"},
                {"value": min_year+(0.75*range_year), "label": "75%"},
            ],
            mb=50,
            minRange=1,
        ),
        dmc.Text(id='range-slider-output'),
    )
#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('Global Labor Attributes', ta='center', style=style_h2),
    dmc.Space(h=30),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(
                dmc.Text('Select a Dataset', size='xl'), span=5, offset=1),
            dmc.GridCol(
                dmc.Text('Adjust min and max year', size='xl'),
                span=5, offset=0),
        ]
    ),
    dmc.Space(h=30),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(get_dataset_radio_picker(), span=5, offset=1),
            dmc.GridCol(get_range_slider(), span=5, offset=0),
        ]
    ),
     html.Hr(style=style_horiz_line),
    dmc.Space(h=30),
    dmc.Grid(  # px_line_data on the left, px_line_norm on the right
        children = [ 
            dmc.GridCol(dcc.Graph(id='px_line_data'), span=5, offset=1),
            dmc.GridCol(dcc.Graph(id='px_line_norm'), span=5, offset=0),
        ]
    ),
])

@app.callback(
    Output('px_line_data', 'figure'),
    Output('px_line_norm', 'figure'),
    Output('range-slider-output', "children"),
    Input('dataset_radio', 'value'),
    Input('range-slider-input', 'value'), 
)
def update(from_radio, x_range):
    dataset_name = from_radio
    df_dataset = get_df(dataset_name)
    px_line_data = get_px_line(
        df_dataset, x_range, title=dataset_name.upper(), norm=False)
    px_line_norm = get_px_line(
        df_dataset, x_range, title=dataset_name.upper(), norm=True)
    return (
        px_line_data,
        px_line_norm, 
        f'{x_range[0]} to {x_range[1]} inclusive'
    )

if __name__ == '__main__':
    app.run(debug=True)