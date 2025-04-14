'''
This code transforms a list of countries associated with each project into 
a list of projects associated with each country. The list of each country
projects is used for this hover display.
'''
import plotly.express as px
import polars as pl

#-------------------------------------------------------------------------------
# df_iso maps ISO3 codes to country names, joined later to df_projects
#-------------------------------------------------------------------------------
df_iso = (
    pl.read_csv('ODL-Export-Countries.csv')
    .select(pl.col('ISO3', 'Country Name'))
    .unique('ISO3')
    .rename({'Country Name' : 'Country'})
)

#-------------------------------------------------------------------------------
# read file with list of projects and associated countries, and tranform to 
# list of countries and associated projects, joined with df_iso for choropleth
#-------------------------------------------------------------------------------
df_projects = (
    pl.read_excel('ODL-Export-projects-1737422266591.xlsx')
    .select(pl.col(['Project Name', 'Countries']))
    .with_columns(pl.col('Project Name').str.slice(0,80)) # limit project len

    # convert countries given as a string into a polars list
    .with_columns(pl.col('Countries').str.split(','))
    .explode('Countries')  # explode is similar to unstack or pandas melt

    # polars list of projects for each country
    .group_by('Countries').agg(pl.col('Project Name')) 
    .with_columns(Project_Count = pl.col('Project Name').list.len())
    .with_columns(Project_Names = pl.col('Project Name').list.join(','))
    .rename({'Countries': 'Country'})

    # replace comma that separates project with carriage return
    .with_columns(pl.col('Project_Names').str.replace_all(',', '<br>'))
    .join(
        df_iso,
        on='Country',
        how='inner'
    )
    .with_columns(pl.col('Country').str.to_uppercase())
    .with_columns(
        Country = pl.col('Country') +
        pl.lit(' - ') +
        pl.col('Project_Count').cast(pl.String) +
        pl.lit(' Project(s)')
    )
    .sort('Project_Count')   # reason for sort is to numerically sort the legend
)

fig = px.choropleth(
    data_frame=df_projects,
    locations='ISO3',
    color='Project_Count',
    color_continuous_scale='viridis',
    hover_name='Country',
    title='List of Projects by Country'.upper(),
    projection='wagner4',
    height=1200, width=1200,
    custom_data=['Country', 'Project_Names'],

)

fig.update_traces(
    hovertemplate="<br>".join([
        "%{customdata[0]}",
        "%{customdata[1]}",
    ])
)
fig.update_layout(
    boxgap=0.25,
    height=500,
    margin=dict(l=10, r=0, b=10, t=100), # , pad=50),
    legend=dict(y=0.5, x=0.85)
)
fig.show()
