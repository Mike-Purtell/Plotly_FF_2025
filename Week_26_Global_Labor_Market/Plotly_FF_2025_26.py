import plotly.express as px
import polars as pl
import polars.selectors as cs
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
from dash_ag_grid import AgGrid
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
style_h3 = {'text-align': 'center', 'font-size': '24px', 
            'fontFamily': 'Arial','font-weight': 'normal'}

legend_font_size = 10
min_year = [
    df_gender_parity_mgmt['YEAR'].min(),
    df_gender_pay_gap['YEAR'].min(),
    df_labor_productivity['YEAR'].min(),
    df_unemployment['YEAR'].min(),
]
max_year = [
    df_gender_parity_mgmt['YEAR'].max(),
    df_gender_pay_gap['YEAR'].max(),
    df_labor_productivity['YEAR'].max(),
    df_unemployment['YEAR'].max()
]
print(f'{min_year = }')
print(f'{max_year = }')

dataset_names = [
    'Management Gender Parity',
    'Gender Pay Gap',
    'Labor Productivity',
    'Unemployment'
]
print(dataset_names)

#----- FUNCTIONS----------------------------------------------------------------
def get_px_line(df, title, norm):
    df = df.select(first:=cs.by_name('YEAR'), ~first)
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
        y_title='Normalized Values'
    else:
        title = title + ' -- RAW DATA'
        y_title='Raw Data Values'
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
        xaxis_title='',  # use blank instead of 'Year'.  Unit is obvious
        yaxis_title=y_title,  # use blank instead of 'Year'.  Unit is obvious
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
            children=dmc.Group([dmc.Radio(l, value=l) for l in dataset_names], my=10),
            id='dataset_radio',
            value=dataset_names[0],
            label="Select Dataset:",
            size="sm",
            mb=10,
        ),
    )

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash()
app.layout =  dmc.MantineProvider([
    dmc.Space(h=30),
    html.Hr(style=style_horiz_line),
    dmc.Text('Global Labor Attributes', ta='center', style=style_h2),
    dmc.Text('', ta='center', style=style_h3, id='zip_code'),
    html.Hr(style=style_horiz_line),
    dmc.Space(h=30),
    dmc.Grid(  
        children = [ 
            dmc.GridCol(get_dataset_radio_picker(), span=2, offset=1),
            # PUT RANGE SLIDER HERE, TO THE RIGHT OF THE DATASET PICKER
        ]
    ),
    dmc.Space(h=30),
    dmc.Grid(  # put px_line_data on the left, px_line_norm on the right
        children = [ 
            dmc.GridCol(dcc.Graph(id='px_line_data'), span=5, offset=1),
            dmc.GridCol(dcc.Graph(id='px_line_norm'), span=5, offset=1),
        ]
    ),
])

@app.callback(
    Output('px_line_data', 'figure'),
    Output('px_line_norm', 'figure'),
    # Output('zip_code', 'children'),
    # Input('zip_code_table', 'cellClicked'),
    #  Input('zip_code_table', 'cellDoubleClicked'),
    # Input('dataset_table', 'cellClicked'),
    Input('dataset_radio', 'value'),
)
def update(from_radio):
    dataset_name = from_radio
    print(f'{dataset_name = }')
    df_dataset = get_df(dataset_name)

    px_line_data = get_px_line(df_dataset, title=dataset_name, norm=False)
    px_line_norm = get_px_line(df_dataset, title=dataset_name, norm=True)
    return px_line_data, px_line_norm

if __name__ == '__main__':
    app.run(debug=True)