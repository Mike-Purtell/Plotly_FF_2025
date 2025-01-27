import plotly.express as px
import pandas as pd

df = pd.read_csv('https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/main/2025/week-3/ODL-Export-Countries.csv')

fig = px.choropleth(
    data_frame=df,
    locations='ISO3',
    color='FA Financing $',
    color_continuous_scale='viridis',
    hover_name='Country Name',
    title='Funded Activities by Country',
    projection='orthographic'
)

fig.update_coloraxes(colorbar_tickprefix='$')
fig.show()