import polars as pl
import plotly.express as px

#----- MAKE PLOT COMPARING PYTHON VS PERL FOR ALL YEARS IN THE DATASET ---------
citation = (
    'Data source from Muhammad Khalid, Kaggle: ' +
     'https://www.kaggle.com/datasets/muhammadkhalid/'   
)
plot_colors = ['red', 'blue']

#----- LOAD AND CLEAN DATA -----------------------------------------------------
df = (
    pl.read_csv(
        'Popularity of Programming Languages from 2004 to 2024.csv',
        ignore_errors=True
        )
    .with_columns(DATE = pl.col('Date').str.to_date(format='%b-%y'))
    # .select('DATE', 'Python', 'Perl')
    .with_columns(pl.all().fill_null(strategy="zero"))
)
prog_lang_list = sorted(
    [lang for lang in list(df.columns) if not lang in['date', 'DATE']]
)

prog_lang_string_1 = (str(prog_lang_list[:8])[1:-1] + ',').replace("'", "")
print(prog_lang_string_1)
prog_lang_string_2 = (str(prog_lang_list[8:16])[1:-1] + ',').replace("'", "")
print(prog_lang_string_2)
prog_lang_string_3 = (str(prog_lang_list[16:24])[1:-1] + ',').replace("'", "")
print(prog_lang_string_3)
prog_lang_string_4 = (str(prog_lang_list[24:32])[1:-1]).replace("'", "")
print(prog_lang_string_4)

#----- PLOT POPULARITY OF PYTHON< PERL FOR ALL YEARS IN THIS DATASET -----------
fig = px.line(
    df,
    'DATE',
    ['Perl', 'Python'],
    template='simple_white',
    title = (
        'Popularity Python vs. Perl, 2004 - 2025<br>' +
        f'<sup>{citation}</sup>' 
        ),
    height=600, width=900,
    color_discrete_sequence=plot_colors,
)
fig.update_layout(
    xaxis_title='',      # x-axis date requires no label, it is obvious
    yaxis_title='Google Search [%]',
    showlegend=False,
    yaxis=dict(
        ticksuffix='%'
    )
)
fig.add_annotation(
    text='Perl',
    xref='x', x=df['DATE'].to_list()[-1], xanchor='left', xshift=10,
    yref='y', y=df['Perl'].to_list()[-1],
    showarrow=False,
    font=dict(color = plot_colors[0], size=16),
)
fig.add_annotation(
    text='Python',
    xref='x', x=df['DATE'].to_list()[-1], xanchor='left', xshift=10,
    yref='y', y=df['Python'].to_list()[-1],
    showarrow=False,
    font=dict(color = plot_colors[1], size=16),
)

annotate_str = f'<b>Language data represents google searches</b><br>'
annotate_str += (prog_lang_string_1 + '<br>')
annotate_str += (prog_lang_string_2 + '<br>')
annotate_str += (prog_lang_string_3 + '<br>')
annotate_str += (prog_lang_string_4 + '<br>')
fig.add_annotation(
    text=annotate_str,
    xref='paper', x=0.05, align='left',
    yref='paper', y=0.9,
    showarrow=False,
    font=dict(color = 'gray', size=12),
)
fig.write_html('Python_v_Perl.html')
fig.show()