"""
DLC Comparison page for comparing statistics across Design Load Cases
"""
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

dash.register_page(__name__, name='DLC Comparison')

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("DLC Comparison", className="mt-4 mb-3"),
            html.P("Compare sensor statistics across Design Load Cases", className="text-muted"),
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Alert(
                "No dataset loaded. Please upload a database file first.",
                id='dlc-no-data-alert',
                color="warning",
                is_open=True
            )
        ])
    ], id='dlc-no-data-row'),
    
    # Control panel
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Comparison Settings")),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Sensor:"),
                            dcc.Dropdown(
                                id='dlc-sensor-dropdown',
                                placeholder="Select a sensor...",
                                clearable=False
                            )
                        ], width=6),
                        dbc.Col([
                            html.Label("Statistic:"),
                            dcc.Dropdown(
                                id='dlc-statistic-dropdown',
                                placeholder="Select a statistic...",
                                clearable=False
                            )
                        ], width=6),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Group By:", className="mt-3"),
                            dcc.RadioItems(
                                id='dlc-groupby-radio',
                                options=[
                                    {'label': ' DLC', 'value': 'dlc'},
                                    {'label': ' Group', 'value': 'group'},
                                ],
                                value='dlc',
                                inline=True
                            )
                        ])
                    ])
                ])
            ])
        ])
    ], className="mb-3", id='dlc-controls-row', style={'display': 'none'}),
    
    # Visualization
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H5("Comparison Plot")),
                dbc.CardBody([
                    dcc.Loading(
                        id="loading-dlc-plot",
                        type="default",
                        children=dcc.Graph(id='dlc-comparison-plot', style={'height': '600px'})
                    )
                ])
            ])
        ])
    ], className="mb-3", id='dlc-plot-row', style={'display': 'none'}),
])


@callback(
    [Output('dlc-no-data-row', 'style'),
     Output('dlc-controls-row', 'style'),
     Output('dlc-plot-row', 'style'),
     Output('dlc-sensor-dropdown', 'options'),
     Output('dlc-sensor-dropdown', 'value')],
    Input('dataset-metadata', 'data'),
    State('dataset-store', 'data'),
    prevent_initial_call=False
)
def initialize_dlc_page(metadata, dataset_data):
    """Initialize DLC comparison page"""
    if metadata is None or dataset_data is None:
        return (
            {'display': 'block'},  # Show no-data alert
            {'display': 'none'},   # Hide controls
            {'display': 'none'},   # Hide plot
            [], None
        )
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        # Get sensor names
        sensor_names = sorted(list(set([col[0] for col in df.columns if col[0] != 'filelist'])))
        
        sensor_options = [{'label': name, 'value': name} for name in sensor_names]
        default_sensor = sensor_names[0] if sensor_names else None
        
        return (
            {'display': 'none'},   # Hide no-data alert
            {'display': 'block'},  # Show controls
            {'display': 'block'},  # Show plot
            sensor_options,
            default_sensor
        )
        
    except Exception as e:
        return (
            {'display': 'block'},
            {'display': 'none'},
            {'display': 'none'},
            [], None
        )


@callback(
    [Output('dlc-statistic-dropdown', 'options'),
     Output('dlc-statistic-dropdown', 'value')],
    Input('dlc-sensor-dropdown', 'value'),
    State('dataset-store', 'data'),
    prevent_initial_call=True
)
def update_dlc_statistic_dropdown(sensor_name, dataset_data):
    """Update statistic dropdown for DLC comparison"""
    if sensor_name is None or dataset_data is None:
        return [], None
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        stats = sorted([col[1] for col in df.columns if col[0] == sensor_name])
        
        stat_options = [{'label': name, 'value': name} for name in stats]
        default_stat = stats[0] if stats else None
        
        return stat_options, default_stat
        
    except Exception as e:
        return [], None


@callback(
    Output('dlc-comparison-plot', 'figure'),
    [Input('dlc-sensor-dropdown', 'value'),
     Input('dlc-statistic-dropdown', 'value'),
     Input('dlc-groupby-radio', 'value')],
    State('dataset-store', 'data'),
    prevent_initial_call=True
)
def update_dlc_plot(sensor_name, statistic, groupby, dataset_data):
    """Create DLC comparison plot"""
    if sensor_name is None or statistic is None or dataset_data is None:
        return go.Figure()
    
    try:
        df = pd.read_json(dataset_data, orient='split')
        
        # Get sensor data
        data_col = (sensor_name, statistic)
        if data_col not in df.columns:
            return go.Figure()
        
        # Try to get DLC or group column from filelist
        dlc_col = ('filelist', 'dlc')
        group_col = ('filelist', 'group')
        
        plot_df = pd.DataFrame()
        plot_df['value'] = df[data_col]
        
        # Add grouping column
        if groupby == 'dlc' and dlc_col in df.columns:
            plot_df['category'] = df[dlc_col]
            category_label = 'DLC'
        elif groupby == 'group' and group_col in df.columns:
            plot_df['category'] = df[group_col]
            category_label = 'Group'
        else:
            # Fallback: use index as category
            plot_df['category'] = 'All Files'
            category_label = 'Category'
        
        plot_df = plot_df.dropna(subset=['value'])
        
        # Create box plot grouped by category
        fig = px.box(
            plot_df,
            x='category',
            y='value',
            labels={'category': category_label, 'value': statistic},
            title=f'{sensor_name} - {statistic} by {category_label}',
            points='all'  # Show all points
        )
        
        fig.update_layout(
            template='plotly_white',
            hovermode='closest',
            xaxis_title=category_label,
            yaxis_title=statistic
        )
        
        return fig
        
    except Exception as e:
        return go.Figure().add_annotation(
            text=f"Error creating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
