"""
Plot page for visualizing dataset data via DataSet.plot_stats.
"""
import dash
from dash import callback, dcc, html, Input, Output, State
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from loadex.browser.session_cache import get_dataset


dash.register_page(__name__, path='/plot', name='Plot')


layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Plot Data", className="mt-4 mb-3"),
            html.P(
                "Select x-axis and y-series from filelist metadata and sensor statistics.",
                className="text-muted"
            ),
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='plot-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='plot-no-data-row'),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Plot Settings")),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("X Axis Source"),
                            dcc.Dropdown(
                                id='plot-x-source-dropdown',
                                options=[
                                    {'label': 'filelist', 'value': 'filelist'},
                                    {'label': 'sensor', 'value': 'sensor'},
                                ],
                                value=None,
                                clearable=False,
                                placeholder="Select source"
                            ),
                        ], width=3),
                        dbc.Col([
                            html.Label("Sensor Search"),
                            dcc.Input(
                                id='plot-x-sensor-search',
                                type='text',
                                value='',
                                placeholder='Type sensor name contains...',
                                debounce=True,
                                style={'width': '100%'}
                            ),
                        ], width=4, id='plot-x-sensor-search-col', style={'display': 'none'}),
                        dbc.Col([
                            html.Label("X Sensor"),
                            dcc.Dropdown(
                                id='plot-x-sensor-dropdown',
                                options=[],
                                value=None,
                                clearable=False,
                                placeholder='Select sensor (limited matches)'
                            ),
                        ], width=5, id='plot-x-sensor-dropdown-col', style={'display': 'none'}),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            html.Label("X Axis Parameter / Statistic"),
                            dcc.Dropdown(
                                id='plot-x-stat-dropdown',
                                options=[],
                                value=None,
                                clearable=False,
                                placeholder="Select parameter/statistic"
                            ),
                        ], width=12),
                    ]),
                ])
            ])
        ])
    ], className='mb-3', id='plot-settings-row', style={'display': 'none'}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Y Axis Series (Filter and tick rows)")),
                dbc.CardBody([
                    dag.AgGrid(
                        id='plot-y-grid',
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
                            'rowSelection': {'mode': 'multiRow', 'selectAll': 'filtered',},
                        },
                        style={'height': '40vh', 'width': '100%'}
                    ),
                    html.Div(id='plot-selected-count', className='text-muted mt-2')
                ])
            ])
        ])
    ], className='mb-3', id='plot-grid-row', style={'display': 'none'}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Plot")),
                dbc.CardBody([
                    dcc.Loading(
                        type='default',
                        children=dcc.Graph(id='dataset-plot', style={'height': '65vh'})
                    )
                ])
            ])
        ])
    ], id='plot-figure-row', style={'display': 'none'}),
])


@callback(
    [Output('plot-no-data-row', 'style'),
     Output('plot-settings-row', 'style'),
     Output('plot-grid-row', 'style'),
     Output('plot-figure-row', 'style'),
     Output('plot-x-source-dropdown', 'value'),
     Output('plot-x-stat-dropdown', 'options'),
     Output('plot-x-stat-dropdown', 'value'),
     Output('plot-y-grid', 'columnDefs'),
     Output('plot-y-grid', 'rowData')],
    Input('dataset-metadata', 'data'),
    State('session-id-store', 'data'),
    prevent_initial_call=False
)
def initialize_plot_page(metadata, session_id):
    if metadata is None or session_id is None:
        return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, None, [], None, [], []

    dataset = get_dataset(session_id)
    if dataset is None:
        return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, None, [], None, [], []

    file_df = dataset.filelist.to_dataframe()
    file_stats = [str(col) for col in file_df.columns]

    sensor_df = dataset.sensorlist.to_dataframe()
    y_rows = []

    # filelist rows as selectable y-series
    for statistic in file_stats:
        y_rows.append({
            'source': 'filelist',
            'sensor_name': 'filelist',
            'statistic': statistic,
            'label': f"filelist {statistic}",
        })

    # sensor rows as selectable y-series
    for sensor_name, row in sensor_df.iterrows():
        stats = row.get('stats', []) if hasattr(row, 'get') else []
        if not isinstance(stats, list):
            continue
        for statistic in stats:
            y_rows.append({
                'source': 'sensor',
                'sensor_name': sensor_name,
                'statistic': str(statistic),
                'label': f"{sensor_name} ({statistic})",
            })

    y_column_defs = [
        {'headerName': 'Type', 'field': 'source', 'checkboxSelection': True, 'headerCheckboxSelection': True, 'minWidth': 120, 'pinned': 'left'},
        {'headerName': 'Sensor', 'field': 'sensor_name', 'minWidth': 240},
        {'headerName': 'Statistic', 'field': 'statistic', 'minWidth': 180},
        {'headerName': 'Label', 'field': 'label', 'minWidth': 260},
    ]

    x_stat_options = [{'label': str(statistic), 'value': str(statistic)} for statistic in file_stats]
    default_x_source = 'filelist'
    default_x_stat = 'id' if 'id' in file_stats else (file_stats[0] if file_stats else None)

    return (
        {'display': 'none'},
        {'display': 'block'},
        {'display': 'block'},
        {'display': 'block'},
        default_x_source,
        x_stat_options,
        default_x_stat,
        y_column_defs,
        y_rows,
    )


@callback(
    [Output('plot-x-sensor-search-col', 'style'),
     Output('plot-x-sensor-dropdown-col', 'style')],
    Input('plot-x-source-dropdown', 'value'),
    prevent_initial_call=False
)
def toggle_x_sensor_controls(x_source):
    if x_source == 'sensor':
        return {'display': 'block'}, {'display': 'block'}
    return {'display': 'none'}, {'display': 'none'}


@callback(
    [Output('plot-x-sensor-dropdown', 'options'),
     Output('plot-x-sensor-dropdown', 'value')],
    [Input('plot-x-source-dropdown', 'value'),
     Input('plot-x-sensor-search', 'value')],
    [State('session-id-store', 'data'),
     State('plot-x-sensor-dropdown', 'value')],
    prevent_initial_call=False
)
def update_x_sensor_options(x_source, search_text, session_id, current_sensor):
    if x_source != 'sensor' or session_id is None:
        return [], None

    dataset = get_dataset(session_id)
    if dataset is None:
        return [], None

    sensor_names = [str(name) for name in dataset.sensorlist.names]
    search_text = (search_text or '').strip().lower()

    if search_text:
        matching = [name for name in sensor_names if search_text in name.lower()]
    else:
        matching = sensor_names

    matching = matching[:200]
    options = [{'label': name, 'value': name} for name in matching]
    values = [option['value'] for option in options]

    if current_sensor in values:
        return options, current_sensor
    return options, (values[0] if values else None)


@callback(
    [Output('plot-x-stat-dropdown', 'options', allow_duplicate=True),
     Output('plot-x-stat-dropdown', 'value', allow_duplicate=True)],
    [Input('plot-x-source-dropdown', 'value'),
     Input('plot-x-sensor-dropdown', 'value')],
    [State('session-id-store', 'data'),
     State('plot-x-stat-dropdown', 'value')],
    prevent_initial_call=True
)
def update_x_stat_options(x_source, x_sensor_name, session_id, current_x_stat):
    if x_source is None or session_id is None:
        return [], None

    dataset = get_dataset(session_id)
    if dataset is None:
        return [], None

    if x_source == 'filelist':
        options = [
            {'label': str(statistic), 'value': str(statistic)}
            for statistic in dataset.filelist.to_dataframe().columns
        ]
        values = [opt['value'] for opt in options]
        default_value = 'id' if 'id' in values else (values[0] if values else None)
    else:
        if not x_sensor_name:
            return [], None
        sensor = dataset.sensorlist.get_sensor(x_sensor_name)
        values = [stat.name for stat in sensor.statistics]
        options = [{'label': value, 'value': value} for value in values]
        default_value = values[0] if values else None

    if current_x_stat in values:
        return options, current_x_stat
    return options, default_value


@callback(
    [Output('dataset-plot', 'figure'),
     Output('plot-selected-count', 'children')],
    [Input('plot-x-source-dropdown', 'value'),
     Input('plot-x-sensor-dropdown', 'value'),
     Input('plot-x-stat-dropdown', 'value'),
     Input('plot-y-grid', 'selectedRows')],
    State('session-id-store', 'data'),
    prevent_initial_call=False
)
def update_dataset_plot(x_source, x_sensor_name, x_stat, selected_rows, session_id):
    if session_id is None:
        return go.Figure(), ""

    dataset = get_dataset(session_id)
    if dataset is None:
        return go.Figure(), ""

    if not x_source or not x_stat:
        x_spec = {'name': 'filelist', 'statistic': 'id'}
    elif x_source == 'sensor' and x_sensor_name:
        x_spec = {'name': x_sensor_name, 'statistic': x_stat}
    else:
        x_spec = {'name': 'filelist', 'statistic': x_stat}

    selected_rows = selected_rows or []
    if len(selected_rows) == 0:
        fig = go.Figure()
        fig.add_annotation(
            text="Select one or more rows in the Y Axis Series table.",
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
        )
        fig.update_layout(template='plotly_white')
        return fig, "0 series selected"

    y_specs = []
    for row in selected_rows:
        source = row.get('source')
        statistic = row.get('statistic')
        if source == 'filelist':
            y_specs.append({'name': 'filelist', 'statistic': statistic})
        else:
            y_specs.append({'name': row.get('sensor_name'), 'statistic': statistic})

    try:
        fig = dataset.plot_stats(y=y_specs, x=x_spec, engine='plotly')
        fig.update_layout(template='plotly_white')
        return fig, f"{len(y_specs)} series selected"
    except Exception as error:
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating plot: {str(error)}",
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
        )
        fig.update_layout(template='plotly_white')
        return fig, f"{len(y_specs)} series selected"
