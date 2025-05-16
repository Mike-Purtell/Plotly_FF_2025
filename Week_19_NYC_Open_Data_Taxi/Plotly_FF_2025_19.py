import polars as pl
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc

#----- GLOBAL DATA STRUCTURES --------------------------------------------------
eval_cols = [
    'DRUG_TEST','WAV_COURSE','DEFENSIVE_DRIVING', 'DRIVER_EXAM',
    'MEDICAL_CLEARANCE'
]

color_dict = {
    'COMPLETE' : 'mediumspringgreen',
    'INCOMPLETE' : 'lightcoral'
}

style_space = {
    'border': 'none',
    'height': '4px', 
    'background': 'linear-gradient(to right, #007bff, #ff7b00)', 
    'margin': '10px,',
    'fontsize': 32
}

source_data = (
    'https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/' +
    'main/2025/week-19/TLC_New_Driver_Application.csv'
)

#----- CALLBACK FUNCTION ---- --------------------------------------------------
def get_bar_chart(df, completed_val):
    data_len = dict_complete_len.get(completed_val)
    df_bar = (
        df
        .select(pl.col('REQUIREMENT',completed_val))
        .rename({completed_val:'COMPLETE'})
        .with_columns(
            INCOMPLETE = data_len - pl.col('COMPLETE')
        )
    )

    bar_chart = px.bar(
        df_bar, 
        y='REQUIREMENT', 
        x=['COMPLETE', 'INCOMPLETE'], 
        # height=600, width=900, 
        template='simple_white',
        title=(
            f'COMPLETED {completed_val} of {requirement_num} REQUIREMENTS<br>' +
            f'<sup>Status of {data_len} Applicants</sup>'
        ),
        color_discrete_map=color_dict,
    )
    bar_chart.update_layout(
        yaxis=dict(autorange="reversed"),
        font_size=20,
        legend_title_text='Status'
    )
    bar_chart.update_xaxes(title_text='APPLICANTS')
    bar_chart.update_yaxes(title_text='')
    return(bar_chart)

#----- READ & CLEAN DATASET ----------------------------------------------------
df = (
    pl
    .scan_csv(source_data)
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
    .unique('COMPLETES')
    .sort('COMPLETES')
    .collect()
)

dict_complete_len = dict(zip(df['COMPLETES'], df['LEN']))
completes_list = list(dict_complete_len.keys())
df_transpose = (
    df
    .transpose(
        include_header=True, 
        column_names='COMPLETES',
        header_name='REQUIREMENT', 
    )
    .filter(pl.col('REQUIREMENT') != 'LEN')
    .sort('REQUIREMENT')
)
requirement_num = df_transpose.height

#----- DASH APPLICATION STRUCTURE-----------------------------------------------
app = Dash(external_stylesheets=[dbc.themes.LUX])
app.layout =  dbc.Container([
    html.Hr(),
    html.H2(
        'NYC Taxi Application Status', 
        style={'text-align': 'center', 'font-size': '32px'}
    ),
    html.Hr(style=style_space),
    html.Div([
        html.P(
            'Radio buttons select # of completed requirements', 
            style={
                'text-align': 'center', 
                'margin-left': '80px', 
                'margin-right': '80px', 
                'font-size': '32px',
                'color': 'gray',
            }
        ),
        html.Hr(style=style_space)
    ]),
    dbc.Col(
        [
            dbc.RadioItems(
                id='radio_sel',
                options=completes_list,
                value = '4',
                inline=True,
                labelStyle={
                    'margin-right': '30px',  
                    'font-size': '20px',
                }
            )
        ]
    ),
    html.Div(id='dd-output-container'),
    dbc.Row([
        dbc.Col(dcc.Graph(id='bar_chart')),
    ]),
])

@app.callback(
    Output('bar_chart', 'figure'),
    Input('radio_sel', 'value'),
)
def update_dashboard(completed_val):
    return (
        get_bar_chart(df_transpose, completed_val)
    )
if __name__ == '__main__':
    app.run_server(debug=True)