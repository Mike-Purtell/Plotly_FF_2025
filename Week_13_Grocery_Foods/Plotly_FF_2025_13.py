from dash import Dash, dcc
import dash_bootstrap_components as dbc
# import dash_ag_grid as dag
import polars as pl
import plotly.graph_objects as go
# ASK Adam why original code had graph_objs instead of graph_objects??? 

# Download CSV sheet at: 
# https://drive.google.com/file/d/1EoFTpSJOIYmVzemoMLj7vMTqeM1zMy0o/view?usp=sharing

df = pl.read_csv('GroceryDB_foods.csv')
df_filtered = (
    df
    .select(  # choose and re-name columns to keep, clean up CATEGORY DATA 
      CATEGORY=pl.col('harmonized single category')
                .str.to_titlecase()
                .str.replace_all('-', ' ')
                .str.replace('Milk Milk Substitute', 'Milk or Substitute'),
      PROTEIN=pl.col('Protein'),
      FAT_TOTAL=pl.col('Total Fat'),
      CARBS=pl.col('Carbohydrate'),
      SUGAR_TOTAL=pl.col('Sugars, total'),
      DIETARY_FIBER=pl.col('Fiber, total dietary'),
      CALCIUM=pl.col('Calcium'),
      IRON=pl.col('Iron'),
      SODIUM=pl.col('Sodium'),
      VITAMIN_C=pl.col('Vitamin C'),
      CHOLESTEROL=pl.col('Cholesterol'),
      SAT_FATTY_ACIDS_TOT=pl.col('Fatty acids, total saturated'),
      VITAMIN_A=pl.col('Total Vitamin A'),     
    )
    # .filter(
    #     pl.col('CATEGORY')
    #     .is_in(['drink-tea','drink-soft-energy-mixes','drink-juice','drink-shakes-other'])
    # )
    .group_by('CATEGORY', maintain_order=True)
    .mean()
)
# these lists will be used as callback choices
category_list = sorted(df_filtered['CATEGORY'].unique().to_list())
print(f'{len(category_list) = }')
print(category_list)

nutrition_list = sorted([c for c in df.columns if c!= 'CATEGORY'])
print(f'{len(nutrition_list) = }')
print(nutrition_list)

print(df_filtered)
print(1/0)

#----- DEFINE FUNCTIONS---------------------------------------------------------
def make_fig():
  fig = go.Figure()
  fig.add_trace(go.Scatterpolar(
        r=df_filtered['Fiber, total dietary'],
        theta=df_filtered['harmonized single category'],
        fill='toself',
        name='Dietary Fiber'
  ))
  fig.add_trace(go.Scatterpolar(
        r=df_filtered['Fatty acids, total saturated'],
        theta=df_filtered['harmonized single category'],
        fill='toself',
        name='Saturated Fatty Acids'
  ))

  fig.update_layout(
    polar=dict(
      radialaxis=dict(
        visible=True,
      )),
    showlegend=True,
      legend=dict(
          title=dict(
              text="Average Ingredient Count"
          )
      )
  )

app.layout = [
    dcc.Graph(figure=fig)
]

@app.callback(
        Output('scatterpolar_fig', 'figure'),
        Input('lang_dropdown', 'value'),
        Input('lang_dropdown', 'value'),
)
def update_dashboard(selected_values):
    df_selected = df.select(pl.col('DATE', selected_values[0], selected_values[1]))
    line_plot = create_line_plot(selected_values[0], selected_values[1])
    scatter_plot  = create_scatter_plot(selected_values[0], selected_values[1])
    return line_plot, scatter_plot, df_selected.to_dicts()

app = Dash(external_stylesheets=[dbc.themes.SANDSTONE])

#----- RUN THE APP -------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)