"""
Time series plotting page using File.plot_timeseries.
"""
import dash
from dash import callback, dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.colors import qualitative

from loadex.browser.session_cache import get_dataset


dash.register_page(__name__, path='/timeseries', name='Time Series')


def _wrap_axis_title(text: str, max_chars: int = 20) -> str:
    """Wrap long axis titles with HTML line breaks for Plotly."""
    if not text or len(text) <= max_chars:
        return text

    words = text.split(" ")
    if len(words) == 1:
        return "<br>".join(
            text[index:index + max_chars]
            for index in range(0, len(text), max_chars)
        )

    lines = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return "<br>".join(lines)


def _sensor_axis_label(sensor) -> str:
    """Build axis label with optional unit metadata."""
    metadata = getattr(sensor, 'metadata', {}) or {}
    unit = metadata.get('unit')
    if unit in (None, ""):
        unit = metadata.get('units')

    if unit in (None, ""):
        return sensor.name
    return f"{sensor.name} [{unit}]"


layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Time Series", className="mt-4 mb-3"),
            html.P(
                "Select one or more files and sensors to plot time series data.",
                className="text-muted"
            ),
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='timeseries-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='timeseries-no-data-row'),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Plot Selection")),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Files"),
                            dcc.Dropdown(
                                id='timeseries-file-dropdown',
                                options=[],
                                value=[],
                                multi=True,
                                placeholder='Select files'
                            ),
                        ], md=6),
                        dbc.Col([
                            html.Label("Sensors"),
                            dcc.Dropdown(
                                id='timeseries-sensor-dropdown',
                                options=[],
                                value=[],
                                multi=True,
                                placeholder='Select sensors'
                            ),
                        ], md=6),
                    ], className='g-3'),
                    dbc.Row([
                        dbc.Col([
                            dbc.Checklist(
                                id='timeseries-subplots-toggle',
                                options=[{'label': 'Stack selected sensors into vertical subplots', 'value': 'subplots'}],
                                value=['subplots'],
                                switch=True,
                            ),
                        ], md=12),
                    ], className='g-3 mt-1'),
                ])
            ])
        ])
    ], className='mb-3', id='timeseries-controls-row', style={'display': 'none'}),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Plot")),
                dbc.CardBody([
                    dcc.Loading(
                        type='default',
                        children=dcc.Graph(id='timeseries-plot')
                    ),
                    html.Div(id='timeseries-selection-summary', className='text-muted mt-2')
                ])
            ])
        ])
    ], id='timeseries-figure-row', style={'display': 'none'}),
])


@callback(
    [Output('timeseries-no-data-row', 'style'),
     Output('timeseries-controls-row', 'style'),
     Output('timeseries-figure-row', 'style'),
     Output('timeseries-file-dropdown', 'options'),
     Output('timeseries-file-dropdown', 'value'),
     Output('timeseries-sensor-dropdown', 'options'),
     Output('timeseries-sensor-dropdown', 'value')],
    Input('dataset-metadata', 'data'),
    State('session-id-store', 'data'),
    prevent_initial_call=False
)
def initialize_timeseries_page(metadata, session_id):
    if metadata is None or session_id is None:
        return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}, [], [], [], []

    dataset = get_dataset(session_id)
    if dataset is None:
        return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}, [], [], [], []

    file_options = [
        {'label': file.filepath.name, 'value': str(file.filepath)}
        for file in dataset.filelist
    ]
    sensor_options = [
        {'label': sensor.name, 'value': sensor.name}
        for sensor in dataset.sensorlist
    ]

    default_file_value = [file_options[0]['value']] if file_options else []
    default_sensor_value = [sensor_options[0]['value']] if sensor_options else []

    return (
        {'display': 'none'},
        {'display': 'block'},
        {'display': 'block'},
        file_options,
        default_file_value,
        sensor_options,
        default_sensor_value,
    )


@callback(
    [Output('timeseries-plot', 'figure'),
     Output('timeseries-selection-summary', 'children')],
    [Input('timeseries-file-dropdown', 'value'),
     Input('timeseries-sensor-dropdown', 'value'),
     Input('timeseries-subplots-toggle', 'value')],
    State('session-id-store', 'data'),
    prevent_initial_call=False
)
def update_timeseries_plot(selected_files, selected_sensors, subplot_toggle, session_id):
    fig = go.Figure()

    if session_id is None:
        return fig, ""

    dataset = get_dataset(session_id)
    if dataset is None:
        return fig, ""

    selected_files = selected_files or []
    selected_sensors = selected_sensors or []

    if not selected_files or not selected_sensors:
        fig.add_annotation(
            text="Select at least one file and one sensor.",
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
        )
        fig.update_layout(template='plotly_white')
        return fig, "0 traces selected"

    sensor_map = dataset.sensorlist.to_dict()
    valid_sensors = [sensor_name for sensor_name in selected_sensors if sensor_name in sensor_map]
    use_subplots = bool(subplot_toggle and 'subplots' in subplot_toggle)
    color_palette = qualitative.Plotly
    file_colors = {
        filepath: color_palette[index % len(color_palette)]
        for index, filepath in enumerate(selected_files)
    } if use_subplots else {}

    if not valid_sensors:
        fig.add_annotation(
            text="None of the selected sensors were found in the dataset.",
            x=0.5,
            y=0.5,
            xref='paper',
            yref='paper',
            showarrow=False,
        )
        fig.update_layout(template='plotly_white')
        return fig, "0 traces selected"

    if use_subplots:
        subplot_spacing = 0.3 / max(1, len(valid_sensors))
        fig = go.Figure()

        n_rows = len(valid_sensors)
        bottom_y_axis_ref = 'y' if n_rows == 1 else f'y{n_rows}'
        gap_total = subplot_spacing * max(0, n_rows - 1)
        row_height = (1.0 - gap_total) / n_rows

        # Shared x-axis across all rows to enable cross-subplot hover behavior.
        fig.update_layout(
            xaxis={
                'domain': [0.0, 1.0],
                'anchor': bottom_y_axis_ref,
                'title': {'text': 'Time [s]'},
                'showticklabels': True,
                'showspikes': True,
                'spikemode': 'across',
                'spikesnap': 'cursor',
                'spikethickness': 1,
            }
        )

        for row_index, sensor_name in enumerate(valid_sensors, start=1):
            sensor = sensor_map[sensor_name]
            sensor_axis_label = _sensor_axis_label(sensor)
            top = 1.0 - (row_index - 1) * (row_height + subplot_spacing)
            bottom = top - row_height

            # Avoid tiny floating-point drift outside [0, 1] (e.g. -5.55e-17).
            if row_index == 1:
                top = 1.0
            if row_index == n_rows:
                bottom = 0.0
            top = min(1.0, max(0.0, top))
            bottom = min(1.0, max(0.0, bottom))

            yaxis_layout_key = 'yaxis' if row_index == 1 else f'yaxis{row_index}'
            fig.update_layout({
                yaxis_layout_key: {
                    'domain': [bottom, top],
                    'anchor': 'x',
                    'title': {'text': _wrap_axis_title(sensor_axis_label), 'standoff': 6},
                }
            })

            fig.add_annotation(
                text=sensor_name,
                x=0.5,
                y=min(1.0, top + 0.012),
                xref='paper',
                yref='paper',
                showarrow=False,
                font={'size': 12},
                xanchor='center',
            )
    else:
        fig = go.Figure()

    trace_count = 0
    for row_index, sensor_name in enumerate(valid_sensors, start=1):
        sensor = sensor_map[sensor_name]

        for filepath in selected_files:
            file = dataset.filelist.get_file(filepath)
            subplot_label = file.filepath.name
            overlay_label = f"{file.filepath.name} - {sensor.name}"
            if use_subplots:
                trace_yaxis = 'y' if row_index == 1 else f'y{row_index}'
                fig = file.plot_timeseries(
                    sensor=sensor,
                    axis=fig,
                    label=subplot_label,
                    engine='plotly',
                    line_color=file_colors.get(filepath),
                    legendgroup=filepath,
                    showlegend=(row_index == 1),
                    xaxis_id='x',
                    yaxis_id=trace_yaxis,
                )
            else:
                fig = file.plot_timeseries(
                    sensor=sensor,
                    axis=fig,
                    label=overlay_label,
                    engine='plotly',
                )
            trace_count += 1

    if use_subplots:
        fig.update_layout(height=max(520, 340 * len(valid_sensors)))
    else:
        if len(valid_sensors) == 1:
            yaxis_title = _sensor_axis_label(sensor_map[valid_sensors[0]])
        else:
            yaxis_title = 'Value'
        fig.update_layout(height=700, xaxis_title='Time [s]', yaxis_title=yaxis_title)

    fig.update_layout(
        template='plotly_white',
        showlegend=True,
        hovermode='x unified',
        hoversubplots='axis',
        legend={
            'orientation': 'h',
            'yanchor': 'top',
            'y': -0.2,
            'xanchor': 'left',
            'x': 0.0,
        },
        margin={'b': 120},
    )

    # Draw an x-position guide while hovering.
    fig.update_xaxes(showspikes=True, spikemode='across', spikesnap='cursor', spikethickness=1)
    return fig, f"{trace_count} traces selected"