"""
    Layout File for the Dashboard of the Python Package DBMS Benchmarker
    Copyright (C) 2020  Jascha Jestel

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import dash
#import dash_core_components as dcc
from dash import dcc
#import dash_html_components as html
from dash import html
#import dash_table
from dash import dash_table
import dash_daq as daq
import uuid


def serve_layout(preview) -> html.Div:
    """
    :param preview: The preview DataFrame of all available experiments in result path.
    :return: App layout
    """

    preview = preview.reset_index().rename(columns={'index': 'code'})
    preview['id'] = preview['code']

    stores = html.Div([

        dcc.Store(id='store_session_id', storage_type='session', data=str(uuid.uuid4())),
        dcc.Store(id='store_dashboard_config', storage_type='session', data=dict()),
        dcc.Store(id='store_active_graph', storage_type='session', data=[]),
        dcc.Store(id='store_favorites', storage_type='local'),
        # Wrapping store_loaded_experiment in a loading component causes a loading screen whenever
        # store_loaded_experiment is changed.
        dcc.Loading(
            dcc.Store(id='store_loaded_experiment', storage_type='session', data=-1),
            id='loading_experiment',
            fullscreen=True,
            type='cube'
        ),

        # SIGNALS
        dcc.Store(id='signal_add_graph', storage_type='memory'),
        dcc.Store(id='signal_update_graphs', storage_type='memory'),
        dcc.Store(id='signal_resize', storage_type='memory'),
        dcc.Store(id='signal_move', storage_type='memory'),
        dcc.Store(id='signal_pre_settings', storage_type='memory'),
        dcc.Store(id='signal_update_settings', storage_type='memory'),
        dcc.Store(id='signal_delete', storage_type='memory'),
        dcc.Store(id='signal_load_favorite', storage_type='memory'),
    ])

    overlay = html.Div(
        id='experiment_overlay',
        className='side side-right',
        children=html.Div(
            id='experiment_overlay_content',
            children=[
                html.H5('Select Experiment'),
                dash_table.DataTable(id='table_experiment_preview',
                                     sort_action="native",
                                     row_selectable='single',
                                     style_data={
                                         'whiteSpace': 'normal',
                                         'height': 'auto'
                                     },
                                     style_table={
                                         'maxHeight': '80vh',
                                         'overflowY': 'scroll',
                                         'overflowX': 'scroll'
                                     },
                                     style_header={'padding': '10px'},
                                     style_cell={'textAlign': 'left'},
                                     columns=[{'name': i, 'id': i} for i in preview.columns.drop('id')],
                                     data=preview.to_dict('records'),
                                     ),

                daq.BooleanSwitch(
                    on=False,
                    id='load_predefined_dashboard',
                    label="Load predefined dashboard",
                    labelPosition="top",
                    style=dict(marginTop='30px')
                )


            ])
    )

    navbar = html.Div(
        id='navbar',
        children=[
            html.Button('Filter', id='btn_toggle_filter'),

            html.Div(
                id='favorites_container',
                children=[
                    html.Button('Favorites', id='btn_favorites'),
                    html.Div(
                        id='favorites_content',
                        children=[
                            html.Div(
                                id='favorites_table',
                                children=[
                                    dash_table.DataTable(
                                        id='table_favorites',
                                        sort_action="native",
                                        row_selectable='single',
                                        style_data={
                                            'whiteSpace': 'normal',
                                            'height': 'auto',
                                        },
                                        style_table={'overflow': 'auto'},
                                        style_cell={'textAlign': 'left'},
                                        style_header={'padding': '10px'},
                                        columns=[{'name': 'ID', 'id': 'fav_id'},
                                                 {'name': 'Experiment Code', 'id': 'code'},
                                                 {'name': 'Creation Time', 'id': 't_create'},
                                                 {'name': 'Comment', 'id': 'comment'},
                                                 {'name': 'Number Graphs', 'id': 'number_graphs'}],
                                    ),


                                    html.Button('Load', id='btn_load_favorite',
                                                style=dict(margin='10px 2.5px 10px 0')),

                                    html.Button('Load (append)', id='btn_load_append_favorite',
                                                style=dict(margin='10px 2.5px 10px 2.5px')),

                                    html.Button('Delete', id='btn_remove_favorite',
                                                style=dict(margin='10px 0 10px 2.5px')),

                                ]),

                            dcc.Input(
                                id='input_favorite_comment',
                                autoComplete='off',
                                type='text',
                                placeholder='Enter Comment',
                                style=dict(width='100%', marginTop='10px')),

                            html.Button('Add current Dashboard',
                                        id='btn_add_favorite',
                                        style=dict(marginTop='10px')),

                            html.A('Download Favorites',
                                   id='download_favorites',
                                   className='button',
                                   style=dict(display='inline-block', margin='10px 0 0 5px', width='auto'),
                                   download="",
                                   href="",
                                   target="_blank"),

                            dcc.Upload(
                                id='upload_favorites',
                                children=html.Div([
                                    'Upload Favorites - ',
                                    html.A('Select File')
                                ]),
                                style={
                                    'width': '100%',
                                    'height': '35px',
                                    'lineHeight': '35px',
                                    'border': 'dashed 1px grey',
                                    'borderRadius': '5px',
                                    'textAlign': 'center',
                                    'marginTop': '10px',
                                },
                                multiple=False,
                                max_size=10000,
                            ),

                            dcc.ConfirmDialog(
                                id='upload_alert',
                                message='Please upload JSON file for current experiment.',
                            ),

                        ])
                ]),

            html.Button('Select Experiment', id='btn_toggle_experiment_overlay'),

            html.Button('Experiment Info', id='btn_experiment_info'),

            html.Button('Activate all', id='btn_activate_all'),

            html.Button('Close all active', id='btn_close_all_active'),

            html.Button(id='btn_test', style=dict(display='none')),

            html.Div(
                id='add_graph_container',
                children=[
                    html.Button('Add Graph', id='add_graph_button'),
                    html.Div(
                        id='add_graph_content',
                        children=[
                            html.Button('Add empty Graph', id='btn_add_empty'),
                            html.Button('Duplicate Graph', id='btn_duplicate_graph'),
                            html.Button('Add preset Graph', id='btn_add_preset'),
                            html.Button('Add Statistics Table', id='btn_add_table'),
                            html.Button('Investigate Q1', id='btn_add_investigate_q1'),
                        ])
                ]),

            html.Div(
                id='div_workflow_name',
                style=dict(display='inline')
            ),

            html.Button('Settings', id='btn_toggle_settings', style=dict(float='right', marginRight='20px')),
        ]
    )

    filter_side = html.Div(
        id='filter_side',
        className='sidenav',
        children=[

            html.Div([
                html.H5('DBMS / Connections', id='heading_table_dbms'),

                dash_table.DataTable(id='table_dbms',
                                     style_data={
                                         'whiteSpace': 'normal',
                                         'height': 'auto'
                                     },
                                     row_selectable='multi',
                                     style_cell={'textAlign': 'left'},
                                     page_action='none',
                                     style_table={
                                         'maxHeight': '350px',
                                         'overflow': 'scroll',
                                     },

                                     ),

                html.Div(id='btn_select_all_connections', className='icon-btn'),
                html.Div(id='btn_deselect_all_connections', className='icon-btn'),
                html.Div(id='btn_connection_info', className='icon-btn'),

                html.H5('Filter Connections'),
                html.Label("DBMS"),
                dcc.Dropdown(
                    id='dropdown_dbms',
                    multi=True
                ),
                html.Label("Node"),
                dcc.Dropdown(
                    id='dropdown_node',
                    multi=True
                ),
                html.Label("Client"),
                dcc.Dropdown(
                    id='dropdown_client',
                    multi=True
                ),
                html.Label("GPU"),
                dcc.Dropdown(
                    id='dropdown_gpu',
                    multi=True
                ),
                html.Label("CPU"),
                dcc.Dropdown(
                    id='dropdown_cpu',
                    multi=True,
                ),

                html.Button('Apply Filter', id='btn_apply_filter'),

            ]),

            html.Div([

                html.H5('Queries'),
                dash_table.DataTable(id='table_query',
                                     style_data={
                                         'whiteSpace': 'normal',
                                         'height': 'auto'
                                     },
                                     row_selectable='single',
                                     style_cell={'textAlign': 'left'},
                                     # fixed_rows={'headers': True, 'data': 0},
                                     page_action='none',
                                     style_table={
                                         'maxHeight': '300px',  # TODO max height
                                         'overflowY': 'scroll',
                                         'overflowX': 'scroll'
                                     },
                                     ),

                html.Div(id='btn_deselect_query', className='icon-btn'),
                html.Div(id='btn_query_info', className='icon-btn'),
            ])
        ]
    )

    settings_side = html.Div(
        id='settings_side',
        className='sidenav',
        children=[

            html.Div(
                id='parent_radio_preset_graph',
                children=[

                    dcc.RadioItems(
                        id='radio_preset_graph',
                        options=[
                            dict(label='Heatmap Errors', value='heatmap_errors'),
                            dict(label='Heatmap Warnings', value='heatmap_warnings'),
                            dict(label='Heatmap Result Set Size', value='heatmap_result_set_size'),
                            dict(label='Heatmap Total Time', value='heatmap_total_time'),
                            dict(label='Heatmap Latency Run', value='heatmap_latency_run'),
                            dict(label='Heatmap Throughput Run', value='heatmap_throughput_run'),
                            dict(label='Heatmap Timer Run Factor', value='heatmap_timer_run_factor'),
                            dict(label='Bar Chart Run drill-down', value='barchart_run_drill'),
                            dict(label='Bar Chart Ingestion Time', value='barchart_ingestion_time')
                        ]
                    )
                ]
            ),


            html.Div(
                id='div_graph_settings',
                children=[

                    html.H5('Settings', id='heading_settings'),
                    html.Label('Type', id='label_type'),
                    dcc.Dropdown(
                        id='dd_type',
                        options=[
                            {'label': x, 'value': x} for x in ['timer', 'latency', 'throughput', 'monitoring']
                        ],
                    ),

                    html.Label('Name', id='label_name'),
                    dcc.Dropdown(
                        id='dd_name',
                    ),

                    html.Label('Aggregate Run', id='label_query_aggregate'),
                    dcc.Dropdown(
                        id='dd_query_aggregate',
                        options=[
                            {'label': x, 'value': x} for x in
                            ['factor', 'Mean', 'Std Dev', 'cv [%]', 'Median', 'iqr', 'qcod [%]', 'Min', 'Max', 'Range', 'Geo', '1st', 'Last', 'Sum', 'P25', 'P75', 'P90', 'P95']
                        ],
                    ),

                    html.Label('Aggregate Query', id='label_total_aggregate'),
                    dcc.Dropdown(
                        id='dd_total_aggregate',
                        options=[
                            {'label': x, 'value': x} for x in
                            ['factor', 'Mean', 'Std Dev', 'cv [%]', 'Median', 'iqr', 'qcod [%]', 'Min', 'Max', 'Range', 'Geo', '1st', 'Last', 'Sum', 'P95', 'P25', 'P75', 'P90', 'P95']
                        ],
                    ),

                    html.Label('Aggregate Config', id='label_connection_aggregate'),
                    dcc.Dropdown(
                        id='dd_connection_aggregate',
                        options=[
                            {'label': x, 'value': x} for x in
                            ['factor', 'Mean', 'Std Dev', 'cv [%]', 'Median', 'iqr', 'qcod [%]', 'Min', 'Max', 'Range', 'Geo', '1st', 'Last', 'Sum', 'P95', 'P25', 'P75', 'P90', 'P95']
                        ],
                    ),

                    html.Label('Graph Type', id='label_graph_type'),
                    dcc.Dropdown(
                        id='dd_graph_type',
                        options=[{'label': x, 'value': x} for x in
                                 ['Heatmap', 'Boxplot', 'Line Chart', 'Histogram', 'Bar Chart',
                                  'Table Measures', 'Table Statistics', 'Preset']]
                    ),

                    html.Div(
                        id='div_warmup',
                        children=[
                            html.Label('Number of warmup runs', htmlFor='slider_warmup', id='label_slider_warmup'),
                            dcc.Slider(
                                id='slider_warmup',
                                min=0,
                                max=10,
                                step=1,
                                value=0,
                                marks={x: str(x) for x in range(11)},
                            )
                        ]
                    ),

                    html.Div(
                        id='div_cooldown',
                        children=[
                            html.Label('Number of cooldown runs', htmlFor='slider_cooldown', id='label_slider_cooldown'),
                            dcc.Slider(
                                id='slider_cooldown',
                                min=0,
                                max=10,
                                step=1,
                                value=0,
                                marks={x: str(x) for x in range(11)},
                            )
                        ]
                    ),

                    html.Label('X-Axis', id='label_graph_xaxis'),
                    dcc.Dropdown(
                        id='dd_graph_xaxis',
                        options=[{'label': x, 'value': x} for x in ['Query', 'Connection']],
                        value='Query',
                        clearable=False
                    ),

                    html.Label('Color by', id='label_graph_colorby'),
                    dcc.Dropdown(
                        id='dd_graph_colorby',
                        options=[{'label': x, 'value': x} for x in ['DBMS', 'Node', 'Script', 'CPU Limit', 'Client', 'GPU', 'CPU', 'Docker Image', 'Experiment Run']],
                    ),

                    html.Label('Boxpoints', id='label_boxpoints'),
                    dcc.Dropdown(
                        id='dd_boxpoints',
                        options=[{'label': x.capitalize(), 'value': x} for x in
                                 ["False", "all", "outliers", "suspectedoutliers"]],
                        value="False"
                    ),

                    html.Label('Sort', id='label_order'),
                    dcc.Dropdown(
                        id='dd_order',
                        options=[{'label': x.capitalize(), 'value': x} for x in
                                 ["trace", "category ascending", "category descending", "total ascending",
                                  "total descending", "min ascending", "min descending", "max ascending",
                                  "max descending", "sum ascending", "sum descending", "mean ascending",
                                  "mean descending", "median ascending", "median descending"]],
                        value='default',
                        clearable=False
                    ),
                    dcc.Checklist(
                        id='checklist_annotate',
                        options=[
                            {'label': 'Annotate', 'value': 'annotate'}
                        ],
                        value=[]
                    ),

                    html.Div(
                        id='div_input_gridColumn_all',
                        children=[
                            html.Label('Grid Columns: '),
                            dcc.Input(
                                id='input_gridColumn_all',
                                type="number",
                                className='',
                                min=3,
                                max=12,
                                step=3,
                                debounce=True,
                            )
                        ]
                    ),

                    html.Div(
                        id='div_input_gridRow_all',
                        children=[
                            html.Label('Grid Rows: '),
                            dcc.Input(
                                id='input_gridRow_all',
                                type="number",
                                className='',
                                min=1,
                                max=3,
                                step=1,
                                debounce=True,
                            ),
                        ]
                    ),

                ]),
        ]
    )

    graph_grid = html.Div(
        id='graph_grid',
        children=[]
    )

    content = html.Div(
        id='content',
        children=[
            filter_side,
            settings_side,
            graph_grid
        ])

    modal = html.Div(
        id='info_modal',
        children=[
            html.Span(id='span_close_modal', children='x'),
            html.Div(
                id='info_modal_body',
                children=[]
            )
        ]
    )

    layout = html.Div([
        stores,
        overlay,
        navbar,
        content,
        modal,
    ])

    return layout
