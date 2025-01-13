import polars as pl
import plotly.express as px

#-------------------------------------------------------------------------------
#    Read and clean the data
#-------------------------------------------------------------------------------
scatter_matrix_cols = [
    'DEHP_ng_serving',  'DBP_ng_serving','DEP_ng_serving', 'BBP_ng_serving']
df_scatter_matrix = (
    pl.scan_csv('samples.tsv', separator='\t')
    .select(pl.col(['shipped_in'] + scatter_matrix_cols))
    .with_columns(pl.col(scatter_matrix_cols).str.replace(r'<',''))
    .with_columns(pl.col(scatter_matrix_cols).str.replace(r'>',''))
    .with_columns(pl.col(scatter_matrix_cols).cast(pl.Float64))
    .filter(pl.col('DBP_ng_serving') < 8000)
    .filter(pl.col('DEHP_ng_serving') < 5000)  # adam filtered on 20K
    .collect()
)

#-------------------------------------------------------------------------------
#    Generate the scatter_matrix
#-------------------------------------------------------------------------------
fig = px.scatter_matrix(
    df_scatter_matrix,
    dimensions=scatter_matrix_cols,
    template='simple_white',
    height=1000, width=1200,
    title=(
        'Bay Area Foods with Plastic - px.scatter_matrix' + 
        "<br><sup>Data Source: PlasticList. 'Data on Plastic Chemicals in Bay Area Foods'." +  
        '  plasticlist.org. Accessed Jan 10, 2025.</sup>'
    ),
    color='shipped_in'
)
fig.update_layout(title=dict(font=dict(size=22)))

# remove plots from the grid diagonal, and from the upper half
fig.update_traces(
    diagonal_visible=False,
    showupperhalf=False,
    marker=dict(size=5),
)

# move the legend closer to the data
fig.update_layout(
        legend=dict(
        yanchor='middle',
        y=0.9,
        xanchor="left",
        x=0.4,
        font=dict(size=16)
    )
)
fig.write_html('scatter_matrix.html')
fig.show()
