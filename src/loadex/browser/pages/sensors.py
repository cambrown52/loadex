"""
Sensors page for visualizing sensor statistics and comparisons
"""
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

dash.register_page(__name__, name='Sensors')

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Sensor Visualization", className="mt-4 mb-3"),
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='sensors-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='sensors-no-data-row'),
    
    # Control panel
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Select Sensors and Statistics")),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Sensor:"),
                            dcc.Dropdown(
                                id='sensor-dropdown',
                                placeholder="Select a sensor...",
                                clearable=False
                            )
                        ], width=6),
                        dbc.Col([
                            html.Label("Statistic:"),
                            dcc.Dropdown(
                                id='statistic-dropdown',
                                placeholder="Select a statistic...",
                                clearable=False
                            )
                        ], width=6),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Plot Type:", className="mt-3"),
                            dcc.RadioItems(
                                id='plot-type-radio',
                                options=[
                                    {'label': ' Bar Chart', 'value': 'bar'},
                                    {'label': ' Box Plot', 'value': 'box'},
                                    {'label': ' Scatter Plot', 'value': 'scatter'},
                                ],
                                value='bar',
                                inline=True
                            )
                        ])
                    ])
                ])
            ])
        ])
    ], className="mb-3", id='sensors-controls-row', style={'display': 'none'}),
    
    # Visualization area
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Sensor Data Visualization")),
                dbc.CardBody([
                    dcc.Loading(
                        id="loading-sensor-plot",
                        type="default",
                        children=dcc.Graph(id='sensor-plot', style={'height': '600px'})
                    )
                ])
            ])
        ])
    ], className="mb-3", id='sensors-plot-row', style={'display': 'none'}),
    
    # Statistics table
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Statistics Summary")),
                dbc.CardBody([
                    html.Div(id='sensor-stats-table')
                ])
            ])
        ])
    ], className="mb-3", id='sensors-table-row', style={'display': 'none'}),
])


@callback(
    [Output('sensors-no-data-row', 'style'),
     Output('sensors-controls-row', 'style'),
     Output('sensors-plot-row', 'style'),
     Output('sensors-table-row', 'style'),
     Output('sensor-dropdown', 'options'),
     Output('sensor-dropdown', 'value')],
    Input('dataset-metadata', 'data'),
    State('dataset-store', 'data'),
    prevent_initial_call=False
)
def initialize_sensors(metadata, dataset_data):
    """Initialize sensor dropdowns when dataset is loaded"""
    if metadata is None or dataset_data is None:
        return (
            {'display': 'block'},  # Show no-data alert
            {'display': 'none'},   # Hide controls
            {'display': 'none'},   # Hide plot
            {'display': 'none'},   # Hide table
            [], None
        )
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        # Get sensor names from MultiIndex columns (exclude 'filelist')
        sensor_names = sorted(list(set([col[0] for col in df.columns if col[0] != 'filelist'])))
        
        sensor_options = [{'label': name, 'value': name} for name in sensor_names]
        default_sensor = sensor_names[0] if sensor_names else None
        
        return (
            {'display': 'none'},   # Hide no-data alert
            {'display': 'block'},  # Show controls
            {'display': 'block'},  # Show plot
            {'display': 'block'},  # Show table
            sensor_options,
            default_sensor
        )
        
    except Exception as e:
        return (
            {'display': 'block'},
            {'display': 'none'},
            {'display': 'none'},
            {'display': 'none'},
            [], None
        )


@callback(
    [Output('statistic-dropdown', 'options'),
     Output('statistic-dropdown', 'value')],
    Input('sensor-dropdown', 'value'),
    State('dataset-store', 'data'),
    prevent_initial_call=True
)
def update_statistic_dropdown(sensor_name, dataset_data):
    """Update statistic dropdown based on selected sensor"""
    if sensor_name is None or dataset_data is None:
        return [], None
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        # Get statistics for selected sensor
        stats = sorted([col[1] for col in df.columns if col[0] == sensor_name])
        
        stat_options = [{'label': name, 'value': name} for name in stats]
        default_stat = stats[0] if stats else None
        
        return stat_options, default_stat
        
    except Exception as e:
        return [], None


@callback(
    Output('sensor-plot', 'figure'),
    [Input('sensor-dropdown', 'value'),
     Input('statistic-dropdown', 'value'),
     Input('plot-type-radio', 'value')],
    State('dataset-store', 'data'),
    prevent_initial_call=True
)
def update_plot(sensor_name, statistic, plot_type, dataset_data):
    """Update the visualization based on selections"""
    if sensor_name is None or statistic is None or dataset_data is None:
        return go.Figure()
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        # Extract the data for the selected sensor and statistic
        data_col = (sensor_name, statistic)
        if data_col not in df.columns:
            return go.Figure()
        
        plot_data = df[data_col].dropna()
        
        # Create the plot based on type
        if plot_type == 'bar':
            fig = px.bar(
                x=plot_data.index,
                y=plot_data.values,
                labels={'x': 'File Index', 'y': statistic},
                title=f'{sensor_name} - {statistic}'
            )
        elif plot_type == 'box':
            fig = px.box(
                y=plot_data.values,
                labels={'y': statistic},
                title=f'{sensor_name} - {statistic} Distribution'
            )
        else:  # scatter
            fig = px.scatter(
                x=plot_data.index,
                y=plot_data.values,
                labels={'x': 'File Index', 'y': statistic},
                title=f'{sensor_name} - {statistic}'
            )
        
        fig.update_layout(
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig
        
    except Exception as e:
        return go.Figure().add_annotation(
            text=f"Error creating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )


@callback(
    Output('sensor-stats-table', 'children'),
    [Input('sensor-dropdown', 'value'),
     Input('statistic-dropdown', 'value')],
    State('dataset-store', 'data'),
    prevent_initial_call=True
)
def update_stats_table(sensor_name, statistic, dataset_data):
    """Update statistics summary table"""
    if sensor_name is None or statistic is None or dataset_data is None:
        return html.P("Select a sensor and statistic to view summary", className="text-muted")
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        data_col = (sensor_name, statistic)
        if data_col not in df.columns:
            return html.P("No data available", className="text-muted")
        
        data = df[data_col].dropna()
        
        # Calculate summary statistics
        summary = pd.DataFrame({
            'Statistic': ['Count', 'Mean', 'Std Dev', 'Min', '25%', '50%', '75%', 'Max'],
            'Value': [
                len(data),
                data.mean(),
                data.std(),
                data.min(),
                data.quantile(0.25),
                data.quantile(0.50),
                data.quantile(0.75),
                data.max()
            ]
        })
        
        return dbc.Table.from_dataframe(
            summary,
            striped=True,
            bordered=True,
            hover=True,
            size='sm'
        )
        
    except Exception as e:
        return html.P(f"Error: {str(e)}", className="text-danger")
