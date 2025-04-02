from dash import Dash, dcc
import dash_ag_grid as dag
import pandas as pd
import plotly.graph_objs as go

# Download CSV sheet at: https://drive.google.com/file/d/1EoFTpSJOIYmVzemoMLj7vMTqeM1zMy0o/view?usp=sharing
df = pd.read_csv('GroceryDB_foods.csv')

df_filtered = df[df['harmonized single category'].isin(['drink-tea','drink-soft-energy-mixes','drink-juice','drink-shakes-other'])]
df_filtered = df_filtered.groupby(["harmonized single category"])[['Fatty acids, total saturated', 'Fiber, total dietary']].mean().reset_index()

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

grid = dag.AgGrid(
    rowData=df.to_dict("records"),
    columnDefs=[{"field": i, 'filter': True, 'sortable': True} for i in df.columns],
    dashGridOptions={"pagination": True},
    columnSize="sizeToFit"
)

app = Dash()
app.layout = [
    grid,
    dcc.Graph(figure=fig)
]


if __name__ == "__main__":
    app.run(debug=True)