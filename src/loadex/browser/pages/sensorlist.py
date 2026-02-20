"""
Sensor list page for displaying all dataset.sensorlist entries in a Dash AG Grid table.
"""
import dash
from dash import callback, html, Input, Output, State
import dash_ag_grid as dag
import dash_bootstrap_components as dbc

from loadex.browser.session_cache import get_dataset


dash.register_page(__name__, path='/sensorlist', name='Sensor List')


layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Sensor List", className="mt-4 mb-3"),
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='sensorlist-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='sensorlist-no-data-row'),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Dataset Sensor List")),
                dbc.CardBody([
                    dag.AgGrid(
                        id='sensorlist-grid',
                        columnDefs=[],
                        rowData=[],
                        defaultColDef={
                            'sortable': True,
                            'filter': True,
                            'resizable': True,
                            'floatingFilter': True,
                        },
                        dashGridOptions={
                            'pagination': True,
                            'paginationPageSize': 50,
                            'animateRows': False,
                            'tooltipShowDelay': 0,
                        },
                        style={'height': '70vh', 'width': '100%'}
                    )
                ])
            ])
        ])
    ], id='sensorlist-grid-row', style={'display': 'none'}),
])


@callback(
    [Output('sensorlist-no-data-row', 'style'),
     Output('sensorlist-grid-row', 'style'),
     Output('sensorlist-grid', 'columnDefs'),
     Output('sensorlist-grid', 'rowData')],
    Input('dataset-metadata', 'data'),
    State('session-id-store', 'data'),
    prevent_initial_call=False
)
def update_sensorlist_page(metadata, session_id):
    """Render dataset.sensorlist as AG Grid rows/columns."""
    if metadata is None or session_id is None:
        return {'display': 'block'}, {'display': 'none'}, [], []

    dataset = get_dataset(session_id)
    if dataset is None:
        return {'display': 'block'}, {'display': 'none'}, [], []

    sensor_df = dataset.sensorlist.to_dataframe().reset_index()

    # Ensure sensor_name is first column
    if 'sensor_name' in sensor_df.columns:
        sensor_name = sensor_df.pop('sensor_name')
        sensor_df.insert(0, 'sensor_name', sensor_name)

    # Convert NaN/NaT to None for JSON serialization in rowData
    sensor_df = sensor_df.where(sensor_df.notna(), None)

    row_data = sensor_df.to_dict('records')

    column_defs = []
    for column in sensor_df.columns:
        column_def = {
            'headerName': 'Sensor Name' if column == 'sensor_name' else str(column),
            'field': str(column),
            'tooltipField': str(column),
        }
        if column == 'sensor_name':
            column_def['pinned'] = 'left'
            column_def['minWidth'] = 220
        column_defs.append(column_def)

    return {'display': 'none'}, {'display': 'block'}, column_defs, row_data
