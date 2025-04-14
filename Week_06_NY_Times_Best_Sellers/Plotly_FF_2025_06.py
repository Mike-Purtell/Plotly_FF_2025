import polars as pl
import plotly.express as px
import plotly.graph_objects as go

#-------------------------------------------------------------------------------
#   This code groups NY Times best selling books by publisher, and plots their
#   rank over time. You can see how long they stayed on the top 10 list, and 
#   when they peaked. Only books with 10+ weeks in the top 10 are included
#-------------------------------------------------------------------------------
df = (
    pl.scan_csv('NYT Fiction Bestsellers - Bestsellers.csv')
    .with_columns(pl.col('title').str.to_titlecase())
    .with_columns(
        pl.col('bestsellers_date', 'published_date')
        .str.strptime(pl.Date, '%m/%d/%Y')
    )
    .filter(pl.col('rank') <= 10)  # filter out everything below top 10
    .with_columns(  # title_tot is a count of weeks in the top 10 for each book
        title_tot = pl.col('title').count().over(pl.col('title')),
    )
    # filter out books with less than 10 weeks in the top 10
    .filter(pl.col('title_tot') >= 10)  
    .select(
        pl.col(
            'author', 'title',  'publisher', 'bestsellers_date',
            'rank', 'weeks_on_list'
        )
    )
    .collect()
)

publisher_list = sorted(list(set(df['publisher'])))
for publisher_index, publisher in enumerate(sorted(publisher_list),start=1):
    publisher_titles = (
        df.filter(pl.col('publisher') == publisher)
        .unique('title')
        .sort('title')
        ['title']
        .to_list()
    )
    trace_list = []
    my_colors = px.colors.qualitative.Alphabet
    my_color_count = len(my_colors)
    fig = go.Figure()
    for color_index, title in enumerate(sorted(publisher_titles)):
        df_title = (
            df
            .filter(pl.col('title') == title)
            .select(pl.col('rank', 'bestsellers_date'))
        )
        trace = go.Scatter(
            x=df_title['bestsellers_date'],
            y=df_title['rank'],
            mode='lines+markers',
            line=dict(color=my_colors[color_index % my_color_count]),
            marker=dict(color='gray', size = 5),
            name=title,
            line_shape='spline',
            connectgaps=False
        )
        fig.add_trace(trace)
    fig.update_layout(
        template='simple_white', 
        height=400, width=800,
        yaxis=dict(range=[10, 1]),
        title=f'{publisher_index}) Publisher: {publisher}',
        yaxis_title='New York Times Bestsellers Rank',
        showlegend=True
        )
    fig.update_yaxes(
        range=[10.5, 0.5], # 1 on top, 10 on the bottom
        showgrid=True,
        tickvals=[i for i in range(1, 11)],  # 1 to 10 in steps of 1
        ticktext= [f'#{i}'for i in range(1, 11)],
    )  
    fig.show()
