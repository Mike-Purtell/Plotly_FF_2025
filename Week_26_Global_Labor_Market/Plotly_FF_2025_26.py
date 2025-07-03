import plotly.express as px
import polars as pl
import polars.selectors as cs
import dash
import dash_ag_grid as dag
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

#----- GATHER DATA -------------------------------------------------------------
df_gender_parity_mgmt = (
    pl.scan_csv('gender-parity-in-managerial-positions.csv')
    .select(
       YEAR = pl.col('Year').cast(pl.UInt16),
       FEM_SHR_MGMT = pl.col('Female share in management'),
       FEB_SHR_EMP = pl.col('Female share in employment'),
       FEM_SHR_WRKN_POP = pl.col('Female share in the working-age population'),
    )
    .with_columns(pl.col(pl.Float64).cast(pl.Float32))
    .sort('YEAR')
    .collect()
)

df_gender_pay_gap = (
    pl.scan_csv('gender-pay-gap.csv')
    .select(
       YEAR = pl.col('Year').cast(pl.UInt16),
       WORLD = pl.col('World'),
       LOW_INCOME = pl.col('Low income'),
       LOWER_MID = pl.col('Lower-middle income'),
       UPPER_MID = pl.col('Upper-middle income'),
       HIGH_INCOME = pl.col('High income'),
    )
    .with_columns(pl.col(pl.Float64).cast(pl.Float32))
    .sort('YEAR')
    .collect()
)

df_labor_productivity = (
    pl.scan_csv('labor-productivity.csv')
    .select(
       YEAR = pl.col('Year').cast(pl.UInt16),
       WORLD = pl.col('World'),
       AFRICA = pl.col('Africa'),
       AMERICAS = pl.col('Americas'),
       ARAB_STATES = pl.col('Arab States'),
       ASIA_PACIFIC = pl.col('Asia and the Pacific'),
       EUR_CENT_ASIA = pl.col('Europe and Central Asia'),
    )
    .with_columns(pl.col(pl.Float64).cast(pl.Float32))
    .sort('YEAR')
    .collect()
)

df_unemployment = (
    pl.scan_csv('unemployment.csv')
    .select(
       YEAR = pl.col('Year').cast(pl.UInt16),
       WORLD = pl.col('World'),
       AFRICA = pl.col('Africa'),
       AMERICAS = pl.col('Americas'),
       ARAB_STATES = pl.col('Arab States'),
       ASIA_PACIFIC = pl.col('Asia and the Pacific'),
       EUR_CENT_ASIA = pl.col('Europe and Central Asia'),
    )
    .with_columns(pl.col(pl.Float64).cast(pl.Float32))
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
def get_px_line(df, title):
    df_cols = df.columns
    if title.endswith('NORMALIZED'):
        y_title='PERCENT CHANGE'
    else:
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
        xaxis_title='',  # x-axis YEAR not needed, it is obvious
        yaxis_title=y_title, 
        hovermode="x unified",
    )
    return fig

def get_df_raw(dataset_name, x_year_range):
    '''returns requested data set, with years filtered by range slider'''
    if dataset_name == 'Management Gender Parity':
        df = df_gender_parity_mgmt
    elif dataset_name == 'Gender Pay Gap':
        df = df_gender_pay_gap
    elif dataset_name == 'Labor Productivity':
        df = df_labor_productivity
    elif dataset_name == 'Unemployment':
        df =df_unemployment
    else:
        print(f'NO VALID SELECION FOR {dataset_name}')
    return (
        df
        .select(first:=cs.by_name('YEAR'), ~first)
        .filter(pl.col('YEAR') >= x_year_range[0])
        .filter(pl.col('YEAR') <= x_year_range[1])
        )

def get_df_norm(df_to_norm):
    ''' returns dataframe with values normalized'''
    for col in df_to_norm.columns[1:]:  # skips first column, which is the year
        df_to_norm = (
            df_to_norm
            .with_columns(
                ((100*pl.col(col)/pl.col(col).first()-100)).alias(col)
            )
        )
    return df_to_norm
    
def get_dataset_radio_picker():
    ''' radio picker selects desired dataset'''
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
    '''used for setting min and max years to evaluate'''
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

def join_by_year(df1, df2):
    ''' merges raw and normalized datasets for display in dash ag table'''
    df_all = (
        df1.join(
        df2,
        on='YEAR',
        how='left',
        suffix='_NORM'
        )
    )
    float_cols = [c for c in sorted(df_all.columns[1:])]
    df_all_sorted_cols = (['YEAR'] + float_cols)
    df_all = (
        df_all
        .select(df_all_sorted_cols)
    )
    return(df_all)

def get_ag_col_defs(columns):
    ''' return setting for ag columns, with numeric formatting '''
    ag_col_defs = [{   # applies to YEAR column, integer
        'field':'YEAR', 
        'pinned':'left', # pin the YEAR column to keep visible while scrolling
        'width': 100, 
        'suppressSizeToFit': True
    }]
    for col in columns[1:]:   # applies to data columns, floating point
        ag_col_defs.append({
            'headerName': col,
            'field': col,
            'type': "numericColumn",
            'valueFormatter': {"function": "d3.format('.2f')(params.value)"},
            'columnSize':'sizeToFit'
        })
    return ag_col_defs

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
    dmc.Space(h=30),
    dmc.Grid(
        children = [
            dmc.GridCol(
                dag.AgGrid(
                    id='ag_grid'
                ),
                span=10, offset=1
            ),
        ]
    ),
])

@app.callback(
    Output('px_line_data', 'figure'),
    Output('px_line_norm', 'figure'),
    Output('range-slider-output', 'children'),
    Output('ag_grid', 'columnDefs'),  # columns vary by dataset
    Output('ag_grid', 'rowData'),   
    Input('dataset_radio', 'value'),
    Input('range-slider-input', 'value'), 
)
def update(from_radio, x_year_range):
    dataset_name = from_radio
    slider_label = f'{x_year_range[0]} to {x_year_range[1]} inclusive'

    df_raw = get_df_raw(dataset_name, x_year_range)
    px_line_data = get_px_line(
        df_raw, title=dataset_name.upper() + ' -- RAW DATA')
    
    df_norm = get_df_norm(df_raw)
    px_line_norm = get_px_line(
        df_norm, title=dataset_name.upper() + ' -- NORMALIZED')
    
    df_all = join_by_year(df_raw, df_norm)
    ag_col_defs = get_ag_col_defs(df_all.columns)    
    ag_row_data = df_all.to_dicts()

    return (
        px_line_data,
        px_line_norm, 
        slider_label,
        ag_col_defs,
        ag_row_data,
    )

if __name__ == '__main__':
    app.run(debug=True)