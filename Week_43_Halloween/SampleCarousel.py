import dash
from dash import html, dcc
import dash_mantine_components as dmc
import plotly.express as px
dash._dash_renderer._set_react_version('18.2.0')

# Sample data for demonstration
df = px.data.gapminder()

# Create some sample figures
fig1 = px.scatter(
    df[df["year"] == 2007],
    x="gdpPercap",
    y="lifeExp",
    color="continent",
    size="pop",
    hover_name="country",
    log_x=True,
    title="Life Expectancy vs GDP (2007)"
)

fig2 = px.line(
    df[df["country"].isin(["India", "China", "United States"])],
    x="year",
    y="gdpPercap",
    color="country",
    title="GDP per Capita Over Time"
)

fig3 = px.bar(
    df[df["year"] == 2007].groupby("continent", as_index=False)["pop"].sum(),
    x="continent",
    y="pop",
    title="Population by Continent (2007)"
)

# Define the Dash app
app = dash.Dash(__name__, external_stylesheets=dmc.styles.ALL)

app.layout = dmc.MantineProvider(
    theme={"colorScheme": "light"},
    children=dmc.Container(
        [
            dmc.Title(
                "Plotly Dashboard Carousel", 
                # align="center", 
                order=2, 
                mt=20, 
                mb=30
            ),
            dmc.Carousel(
                withIndicators=True,
                height=500,
                slideGap="md",
                align="center",
                children=[
                    dmc.CarouselSlide(
                        dcc.Graph(figure=fig1, style={"height": "450px"})
                    ),
                    dmc.CarouselSlide(
                        dcc.Graph(figure=fig2, style={"height": "450px"})
                    ),
                    dmc.CarouselSlide(
                        dcc.Graph(figure=fig3, style={"height": "450px"})
                    ),
                ],
            ),
        ],
        fluid=True,
        style={"maxWidth": 1000}
    ),
)

if __name__ == "__main__":
    app.run_server(debug=True)
