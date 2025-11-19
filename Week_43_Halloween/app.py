import polars as pl
import polars.selectors as cs
import os
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output
import dash_mantine_components as dmc
dash._dash_renderer._set_react_version('18.2.0')

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
#----- LOAD AND CLEAN THE DATASET ----------------------------------------------
df = (
    pl.read_csv('Halloween.csv')
    .select(
        YEAR = pl.col('Date')
            .str.split('/')
            .list.get(2)
            # .cast(pl.Categorical),
            .cast(pl.UInt16()),
        DAY = pl.col('Day of Week').str.slice(0,3),
        TIME = pl.col('Time').str.replace('pm',' PM'),
        COUNT = pl.col('Count').cast(pl.UInt8()),
    )
)

df_yearly = (
        df
        .group_by(['YEAR'])
        .agg(pl.sum('COUNT').alias('TOTAL_COUNT'))
        .sort('YEAR')
    )
count_2022 = df_yearly.filter(pl.col('YEAR') == 2022).item(0, 'TOTAL_COUNT')
count_2024 = df_yearly.filter(pl.col('YEAR') == 2024).item(0, 'TOTAL_COUNT')
count_2025 = int(count_2024 + 0.5*(count_2024 - count_2022))

df_future = pl.DataFrame({  # predict 2025 by extrapolating from 2022, 2024
        'YEAR':           [2024, 2025],
        'TOTAL_COUNT' :   [count_2024, count_2025]
    })

time_list = sorted(df.unique('TIME')['TIME'].to_list())

time_color_dict = {}
for i, time in enumerate(time_list):
    # time_color_dict[time] = px.colors.qualitative.Light24[i]
    time_color_dict[time] = px.colors.qualitative.Alphabet[i]
    time_color_dict[time] = px.colors.qualitative.D3[i]
    
time_color_dict

#----- DASH COMPONENTS------ ---------------------------------------------------
select_template = (
    dmc.Select(
        label='Pick your favorite Plotly template',
        id='template',
        data= template_list,
        value=template_list[4],
        searchable=False,  # Enables search functionality
        clearable=True,    # Allows clearing the selection
        size='sm',
    ),
)

def get_fig(template):
    fig=px.line(
        df,
        x='YEAR',
        y='COUNT',
        color='TIME',
        color_discrete_map=time_color_dict,
        template = template,
        markers=True,
        title='Halloween TTT<br><sup>Slide 1</sup>',
        height=400, width=800
    )
    fig.update_layout(
        xaxis=dict(
            showline=True, 
            linewidth=1, 
            linecolor='gray', 
            mirror=True,
            showgrid=True,      # show vertical grid lines
            tickmode='linear',  # ensure ticks are evenly spaced
            dtick=1             # one tick/gridline per category
            ),
        yaxis=dict(
            showline=True, 
            linewidth=1, 
            linecolor='gray', 
            mirror=True,
            showgrid=True       # show horizontal grid lines
            ),
        title_y=0.97,
    )
    fig.update_traces(line=dict( width=1))

    # Customize gridlines
    fig.update_xaxes(
        showgrid=True,           # Ensure gridlines are visible
        gridwidth=1,             # Thickness of vertical gridlines
        gridcolor='gray',        # Color of vertical gridlines
    )

    fig.update_yaxes(
        showgrid=True,          # Ensure gridlines are visible
        gridwidth=1,            # Thickness of horizontal gridlines
        gridcolor='gray',        # Color of horizontal gridlines
    )
    fig.update_layout(margin=dict(l=0, r=0, t=50, b=0))

    return fig

def get_carousel_slide(reviewer_text, id):
    slide = dmc.CarouselSlide(
        dmc.Stack([
            dmc.Center(dcc.Graph(figure=go.Figure(), id=id)),
            dmc.Text('Comment:', style=dmc_text_red),
            dmc.Text(reviewer_text, style=dmc_text_gray)
        ])
    )
    return slide

def update_fig(fig, slide_number, template):
    updated_fig = go.Figure(fig)
    if slide_number == 2:   # change the plot title
        updated_fig.update_layout(
            title=dict(
                text=(
                    'Halloween Trick-or-Treaters by Time (TTT)<br>' + 
                    f'<sup>Slide {slide_number}</sup>'
                )
            )
        )
    if slide_number == 3: # change line shape to spline
        updated_fig.update_traces(line=dict(shape='spline'))
        updated_fig.update_layout(title=dict(
            text=(
                'Halloween Trick-or-Treaters by Time (TTT)<br>' +
                f'<sup>Slide {slide_number}</sup>'
            ))
        )
    
    if slide_number == 4: # remove x-axis and y-axis gridlines
        updated_fig.update_xaxes(showgrid=False, zeroline=False)
        updated_fig.update_yaxes(showgrid=False, zeroline=False)
        updated_fig.update_layout(title=dict(
            text=(
                'Halloween Trick-or-Treaters by Time (TTT)<br>' +           
                f'<sup>Slide {slide_number}</sup>'
            ))
        )

    if slide_number == 5: # remove top  and right borders
        updated_fig.update_layout(
            xaxis=dict(
                showline=True, showgrid=False, zeroline=False, mirror=False),
            yaxis=dict(
                showline=True, showgrid=False, zeroline=False, mirror=False),
            title=dict(
                text=(
                    'Halloween Trick-or-Treaters by Time (TTT)<br>'+ 
                    f'<sup>Slide {slide_number}</sup>'
                ))
        )

    if slide_number == 6:    # remove x-axis label
        updated_fig.update_xaxes(title='')
        updated_fig.update_layout(
            title=dict(
                text= ( 
                    'Halloween Trick-or-Treaters by Time (TTT)<br>' + 
                    f'<sup>Slide {slide_number}</sup>'
                ))
        )

    if slide_number == 7:     # remove legend
        updated_fig.update_layout(
            showlegend=False,
            title=dict(
                text=(
                    'Halloween Trick-or-Treaters by Time (TTT)<br>'
                    f'<sup>Slide {slide_number}</sup>'
                )
            )
        )

    if slide_number == 8:     # add color coded annotations
        count_list = df.filter(pl.col('YEAR') == 2024)['COUNT'].to_list()
        updated_fig.update_xaxes(range=[2008, 2025.75])
        y_shift_list = [15, 10, 10, 10, -15, -15]
        for i, shift in enumerate(y_shift_list):
            updated_fig.add_annotation(
                x=1,xref='paper', 
                y=count_list[i], yref='y', 
                text=time_list[i], showarrow=False, 
                font=dict(color=time_color_dict[time_list[i]], size=14), 
                yshift=y_shift_list[i])
        updated_fig.update_layout(
            showlegend=False,
            title=dict(
                text=(
                    'Halloween Trick-or-Treaters by Time (TTT)<br>' + 
                    f'<sup>Slide {slide_number}</sup>'
                )
            )
        )

    if slide_number == 9:     # add vertical line and annotation for covid
        updated_fig.add_vline(
            x=2020, 
            line_width=2, 
            line_dash='dash',
            line_color='gray',
            annotation_text='Covid-19 Pandemic',
        )
        updated_fig.update_layout(
            showlegend=False,
            title=dict(
                text=(
                    'Halloween Trick-or-Treaters by Time (TTT)<br>' +
                    f'<sup>Slide {slide_number}</sup>'
                ))
        )
    if slide_number == 10:     # add vertical line for 2013 (heavy rain)
        updated_fig.add_vline(
            x=2013, 
            line_width=2, 
            line_dash='dash',
            line_color='gray',
            annotation_text='1 inch of rain',
        )
        updated_fig.update_layout(
            showlegend=False,
            title=dict(
                text= (
                'Halloween Trick-or-Treaters by Time (TTT)<br>' + 
                f'<sup>Slide {slide_number}</sup>'
                ))
        )

    if slide_number == 11:     # aggregate all time points, single trace by year
        updated_fig=px.line(
            df_yearly,
            x='YEAR',
            y='TOTAL_COUNT',
            template = template,
            markers=True,
            title='Total Halloween Trick-or-Treaters by Year',
            height=400, width=800,
            line_shape='spline',
        )
        updated_fig.update_traces(line=dict( width=1))
        updated_fig.update_layout(margin=dict(l=0, r=0, t=50, b=0))
        updated_fig.add_vline(
            x=2020, 
            line_width=2, 
            line_dash='dash',
            line_color='gray',
            annotation_text='Covid-19 Pandemic',
        )
        updated_fig.add_vline(
            x=2013, 
            line_width=2, 
            line_dash='dash',
            line_color='gray',
            annotation_text='1 inch of rain',
        )
        for i, year in enumerate([2022, 2023, 2024]):
            year_count = (
                df_yearly
                .filter(pl.col('YEAR') == year)
                .item(0, 'TOTAL_COUNT')
            )
            updated_fig.add_annotation(
                x=year,xref='x', 
                y=year_count, yref='y', 
                text=f'{year_count}', showarrow=False, 
                font=dict(color='gray', size=14), 
                yshift=20
                )
        updated_fig.update_xaxes(title='')
        updated_fig.update_layout(
            xaxis=dict(
                showline=True, 
                linewidth=1, 
                linecolor='gray',
                mirror=False, 
                ),
            yaxis=dict(
                showline=True, 
                linewidth=1, 
                linecolor='gray', 
                mirror=False     
                ),
            title_y=0.97,
        )
        updated_fig.update_layout(
            showlegend=False,
            title=dict(
                text=(
                    'Halloween Trick-or-Treaters Aggregated by Year (TTT)<br>'
                    f'<sup>Slide {slide_number}</sup>'
                ))
        )
        updated_fig.update_xaxes(showgrid=False, zeroline=False)
        updated_fig.update_yaxes(showgrid=False, zeroline=False)

    if slide_number == 12:   # aggregate time points, single trace by year
        updated_fig.add_trace(go.Scatter(
            x=df_future['YEAR'],
            y=df_future['TOTAL_COUNT'],
            mode='lines+markers',
            line=dict(
                color='gray',
                dash='dot'
            ),
            marker=dict(size=8),
            name='Gray Dashed Line',
            showlegend=False
        ))
        updated_fig.add_annotation(
            x=2025,xref='x', 
            y=count_2025, yref='y', 
            text=f'{count_2025}', showarrow=False, 
            font=dict(color='red', size=14), 
            yshift=20
            )
        updated_fig.update_layout(
            showlegend=False,
            title=dict(
                text=(
                'Halloween Trick-or-Treaters Aggregated by Year<br>' +
                f'<sup>Slide {slide_number}</sup>'
                ))
        )

    if slide_number == 13:     # aggregate all time points, single trace by year
        df_ordered_days = (
            pl.DataFrame({
                'DAY'     : ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
                'DAY_NUM' : [    1,     2,     3,     4,     5,     6,     7],
                'DAY_COLOR' : ['#d3d3d3']*5 + ['orange', '#d3d3d3'],
            })
        )
        dict_day_color = dict(zip(
            df_ordered_days['DAY'], 
            df_ordered_days['DAY_COLOR']
        ))

        df_avg_by_day = (
            df
            .with_columns(DAY_COUNT = (pl.col('DAY').len().over('DAY')/6))
            .with_columns(
                NORM_DAY_COUNT = (pl.col('COUNT')/(pl.col('DAY_COUNT'))))
            .with_columns(
                DAY_TOTAL = (pl.col('NORM_DAY_COUNT').sum().over('DAY')))
            .unique(['DAY', 'DAY_TOTAL'])
            .select(['DAY', 'DAY_TOTAL'])
            .join(
                df_ordered_days,
                on='DAY',
                how='left'
            )
            .sort('DAY_NUM')
        )
        updated_fig=px.histogram(
            df_avg_by_day,
            x='DAY',
            y='DAY_TOTAL',
            color='DAY',
            color_discrete_map=dict_day_color,
            template = template,
            title=(
                '<b>Call to Action:</b>' +
                'Halloween 2025 is on a Friday (tonight) -- time to shop<br>' + 
                f'<sup>Slide {slide_number}</sup>'
            ),
            height=400, width=800,
        )
        updated_fig.update_layout(
            xaxis=dict(
                showline=True, 
                linewidth=1, 
                linecolor='gray', 
                mirror=False,
                showgrid=False,      # show vertical grid lines
                tickmode='linear',  # ensure ticks are evenly spaced
                dtick=1             # one tick/gridline per category
                ),
            yaxis=dict(
                showline=True, 
                linewidth=1, 
                linecolor='gray', 
                mirror=False,
                showgrid=False       # show horizontal grid lines
                ),
            title_y=0.97,
        )
        updated_fig.update_xaxes(title='')
        updated_fig.update_yaxes(title='NORMALIZED TRICK OR TREATER COJNT PER DAY')
        updated_fig.update_layout(
            margin=dict(l=0, r=0, t=50, b=0),showlegend=False)
                    
    return updated_fig

# #----- DASH APPLICATION STRUCTURE---------------------------------------------
fig=go.Figure()
app = Dash()
server = app.server
app.layout =  dmc.MantineProvider([
    html.Hr(style=style_horizontal_thick_line),
    dmc.Text('Trick or Treating', ta='center', style=style_h2), 
    html.Hr(style=style_horizontal_thick_line), 
    dmc.Grid(children = [dmc.GridCol(select_template, span=3, offset = 1),]),  
    dmc.Space(h=50),
    dmc.Carousel(
        withIndicators=False,
        height=500,
        slideGap='md',
        controlsOffset='sm',
        controlSize=60,
        withControls=True,
        children = [   
            get_carousel_slide(
                'Graph has trick-or-treater counts by time of day and year, ' +
                'but what the heck is TTT?',
                'fig_01'
            ),
            get_carousel_slide(
                'These curves so pointy, can they be smoothed out?',
                'fig_02'
            ),
            get_carousel_slide(
                (
                "Get rid of the gridlines. This is not PowerPoint, " +
                ' use plotly hover tips to interact with the data and ' +
                'show the exact value of any point'
                ),
                'fig_03'
            ),
            get_carousel_slide(
                "Looking better. Top and right borders are not needed, " + 
                "so get rid of them too",
                'fig_04'
            ),
            get_carousel_slide(
                'Remove the x-axis label, "YEAR", it is obvious', 
                'fig_05'
            ),
            get_carousel_slide(
                "Legend is hard to follow with spaghetti-like traces,  " +
                "let's remove it",
                'fig_06'
            ),
            get_carousel_slide(
                "Now I am more confused, hard to tell what is what, " +
                    "can you put the legend back?",
                'fig_07'
            ),
            get_carousel_slide(
                'I could, but this approach with color coded ' +
                'annotations placed near the traces they represent is better. ' +
                'But what happened in 2020?',
                'fig_08'
            ),
            get_carousel_slide(
                'If 2020 was the start of the pandemic, then what happened in 2013?',
                'fig_09'
            ),
            get_carousel_slide(
                'Cincinatti Ohio had heavy rainfall in 2013, affecting turnout. ' + 
                "I don't care about the half-hourly data, " +
                "I want to know much candy to buy for this year.",
                'fig_10'
            ),
            get_carousel_slide(
                "Thanks for combining the data to show yearly totals, and " + 
                "labeling values from 2022 to 2024. Can we extrapolate to get a value for 2025?",
                'fig_11'
            ),
            get_carousel_slide(
                "Extrapolated value for 2025 is at the end of the dashed line, " +
                "which distinguishes future projections from past data." +
                'Halloween is on a Friday (tonight). Do more people show up ' +
                'on Fridays than on weeknights?',
                'fig_12'
            ),
            get_carousel_slide(
                "I thought Friday would be the busiest day for Halloween. This " +
                "data shows that Monday is busiest, Friday is the least busiest. " +
                'Glad I checked',
                'fig_13'
            )
    ])
])

@app.callback(
    Output('fig_01', 'figure'),
    Output('fig_02', 'figure'),
    Output('fig_03', 'figure'),
    Output('fig_04', 'figure'),
    Output('fig_05', 'figure'),
    Output('fig_06', 'figure'),
    Output('fig_07', 'figure'),
    Output('fig_08', 'figure'),
    Output('fig_09', 'figure'),
    Output('fig_10', 'figure'),
    Output('fig_11', 'figure'),
    Output('fig_12', 'figure'),
    Output('fig_13', 'figure'),
    Input('template', 'value'),
)
def callback(template):
    fig_01 = get_fig(template)
    fig_02 = update_fig(fig_01, 2, template)
    fig_03 = update_fig(fig_02, 3, template)
    fig_04 = update_fig(fig_03, 4, template)
    fig_05 = update_fig(fig_04, 5, template)
    fig_06 = update_fig(fig_05, 6, template)
    fig_07 = update_fig(fig_06, 7, template)
    fig_08 = update_fig(fig_07, 8, template)
    fig_09 = update_fig(fig_08, 9, template)
    fig_10 = update_fig(fig_09, 10, template)
    fig_11 = update_fig(fig_10, 11, template)
    fig_12 = update_fig(fig_11, 12, template)
    fig_13 = update_fig(fig_12, 13, template)
    
    return (
        fig_01, fig_02, fig_03, fig_04, fig_05, fig_06, fig_07, fig_08,
        fig_09, fig_10, fig_11, fig_12, fig_13
    )

if __name__ == '__main__':
    app.run(debug=True)