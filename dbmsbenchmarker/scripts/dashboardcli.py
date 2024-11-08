"""
    Web-based Dashboard for the Python Package DBMS Benchmarker
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
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate
from dash import no_update

import plotly.figure_factory as ff
import plotly.graph_objects as go
import pandas as pd

#import layout
from dbmsbenchmarker import *

from flask_caching import Cache
import argparse
import json
import time
import copy
import urllib.parse
import re
import logging
import base64

import dash_auth


# Dash's basic stylesheet
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# Create Dash App instance
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)



# TODO activate caching
# cache = Cache(app.server, config={
#     # Note that filesystem cache doesn't work on systems with ephemeral
#     # filesystems like Heroku.
#     'CACHE_TYPE': 'filesystem',
#     'CACHE_DIR': 'cache-directory',
#
#     # should be equal to maximum number of users on the app at a single time
#     # higher numbers will store more data in the filesystem
#     'CACHE_THRESHOLD': 10
# })


# TODO activate caching by replacing load_experiment function
# @cache.memoize(50)
# def load_experiment(code: str, session_id: str) -> inspector.inspector:
#     """
#     :return: evaluate object with the desired experiment loaded
#     """
#     e = inspector.inspector(result_path)
#     e.load_experiment(code)
#     return e


def load_experiment(code: str) -> inspector.inspector:
    """
    Loads the experiment with given code for global evaluate object.
    This doesn't work well with multiple users exploring different experiments,
    because experiments will reload constantly.
    The advantage of changing the global evaluate variable is the memoization of at least one experiment.
    If the evaluate object works independently, use above load_experiment function.

    :return: evaluate object with the desired experiment loaded
    """

    try:
        if evaluate.get_experiment_workload_properties()['code'] != code:
            evaluate.load_experiment(code)
    except:
        evaluate.load_experiment(code)
    return evaluate


def get_connections_by_filter(filter_by: str, e: inspector.inspector) -> dict:
    """
    Call evaluate.get_experiment_list_connections_by_... method depending on a given filter keyword.
    If the filter keyword is not supported a KeyError is raised.

    :param filter_by: filter keyword to filter connections by
    :param e: evaluate object
    :return: dict like: {'cl-worker4': ['connection1', ‘connection2', ...], ...} containing ALL connections.
    """

    if filter_by == 'DBMS':
        connections_by_filter = e.get_experiment_list_connections_by_dbms()
    elif filter_by == 'Node':
        connections_by_filter = e.get_experiment_list_connections_by_node()
    elif filter_by == 'Script':
        connections_by_filter = e.get_experiment_list_connections_by_script()
    elif filter_by == 'GPU':
        connections_by_filter = e.get_experiment_list_connections_by_hostsystem('GPU')
    elif filter_by == 'Client':
        connections_by_filter = e.get_experiment_list_connections_by_parameter('client')
        #connections_by_filter = e.get_experiment_list_connections_by_connectionmanagement('numProcesses')
    elif filter_by == 'CPU':
        connections_by_filter = e.get_experiment_list_connections_by_hostsystem('CPU')
    elif filter_by == 'CPU Limit':
        connections_by_filter = e.get_experiment_list_connections_by_hostsystem('limits_cpu')
    elif filter_by == 'Docker Image':
        connections_by_filter = e.get_experiment_list_connections_by_parameter('dockerimage')
    elif filter_by == 'Experiment Run':
        connections_by_filter = e.get_experiment_list_connections_by_parameter('numExperiment')
    else:
        raise KeyError('filter_by')

    return connections_by_filter


def get_connection_colors(color_by: str, filter_connections: list, e: inspector.inspector, inc_lists: bool = False):
    """
    Calculates the colors for GIVEN connections using the given color by keyword.
    The filtering for connections is done AFTER get_experiment_list_connection_colors()
    to keep the same color for each connection.
    Returns connection_colors dict like {connection1: #ff0000, ...}
    If inc_lists = True, legend_connections and filter_by_connection are returned too.
    legend_connections is a list of connections to show in legend.
    filter_by_connection is a dict with connections as key and their filter value as value,
    for example: {connection1: cl-worker21, connection2: cl-worker4, ...}
    Todo: Think about optimizing the filter process.

    :param color_by: color by keyword
    :param filter_connections: connections to get color for
    :param e: evaluate object
    :param inc_lists: return legend_connections and filter_by_connection too?
    """

    if color_by is None:
        # if color_by is None -> get color for each connection
        connections_by_filter_sorted = {'temp': e.get_experiment_list_connections()}

    else:
        # get the corresponding dictionary
        connections_by_filter = get_connections_by_filter(color_by, e)

        # sort connections_by_filter values like occurrence in e.get_experiment_list_connections()
        connections_by_filter_sorted = dict()
        for filter_, connections in connections_by_filter.items():
            connections_sorted = []
            for c in e.get_experiment_list_connections():
                if c in connections:
                    connections_sorted.append(c)
            connections_by_filter_sorted[filter_] = connections_sorted

    # get the colors for each connection in connections_by_filter_sorted
    # like {connection1: ff0000, connection2: ff1010, ...}
    connection_colors = e.get_experiment_list_connection_colors(connections_by_filter_sorted)

    # filter connection_colors by given connections if necessary
    if filter_connections:
        connection_colors = {key: connection_colors[key] for key in filter_connections}

    if not inc_lists:
        return tools.anonymize_dbms(connection_colors)

    else:
        # filter connections_by_filter_sorted (the value lists)
        connections_by_filter_sorted_filtered = dict()
        if filter_connections:
            # loop through connections_by_filter_sorted and filter the connections lists based on input connections
            for filter_ in connections_by_filter_sorted:
                if connections_by_filter_sorted[filter_]:
                    connections_filtered = list(filter(lambda c: c in filter_connections, connections_by_filter_sorted[filter_]))
                    if connections_filtered:
                        connections_by_filter_sorted_filtered[filter_] = connections_filtered
        else:
            # no filter necessary
            connections_by_filter_sorted_filtered = connections_by_filter_sorted

        # create legend_connections list and filter_by_connection dictionary
        if color_by is None:
            legend_connections = None
            filter_by_connection = None

        else:
            # use central connection for each filter in legend
            legend_connections = [connections[int(len(connections) / 2 - 0.5)]
                                  for filter_, connections in connections_by_filter_sorted_filtered.items()]

            # "reverse" the connections_by_filter_sorted_filtered dict to
            # receive a dict with connection names as key and their filter as value.
            filter_by_connection = dict()
            for filter_, connections in connections_by_filter_sorted_filtered.items():
                for c in connections:
                    filter_by_connection[c] = filter_

    #return connection_colors, legend_connections, filter_by_connection
    return tools.anonymize_dbms(connection_colors), tools.anonymize_dbms(legend_connections), tools.anonymize_dbms(filter_by_connection)


def sort_df_by_list(df: pd.DataFrame, some_list: list) -> pd.DataFrame:
    """
    Sort given DataFrame like its indices occur in some_list.
    """

    df['temp_order'] = list(map(lambda x: some_list.index(x), df.index))
    df.sort_values(by=['temp_order'], inplace=True)
    df.pop('temp_order')
    return df


def get_new_indices(config: dict, number: int = 1) -> list:
    """
    Get variable number of indices not used in config keys.
    :param config: dict containing dashboard configuration
    :param number: number of new indices
    :return: list of new indices
    """
    max_index = max(list(map(int, config.keys())) + [-1])  # equals -1 when config ist empty
    new_indices = list(range(max_index + 1, max_index + 1 + number))
    return new_indices


def predefined_dashboard(connection_indices: list) -> dict:
    """
    :param connection_indices: all returned graphs include these as their connection_indices attribute.
    :return: dict containing all preset graphs.
    """

    common = dict(graph_type='Preset',
                  connection_indices=connection_indices)
    dashboard = {
        0: Graph(**common,
                 preset='heatmap_errors').__dict__,
        1: Graph(**common,
                 preset='heatmap_warnings').__dict__,
        2: Graph(**common,
                 preset='heatmap_result_set_size').__dict__,
        3: Graph(**common,
                 preset='heatmap_total_time').__dict__,
        4: Graph(**common,
                 preset='heatmap_latency_run',
                 query_aggregate='factor').__dict__,
        5: Graph(**common,
                 preset='heatmap_throughput_run',
                 query_aggregate='factor').__dict__,
        6: Graph(**common,
                 preset='heatmap_timer_run_factor',
                 query_aggregate='factor').__dict__,
        7: Graph(**common,
                 preset='barchart_run_drill',
                 query_aggregate='Mean',
                 total_aggregate='Mean',
                 type='timer').__dict__,
        8: Graph(**common,
                 preset='barchart_ingestion_time').__dict__,
    }
    return dashboard


def investigate_q1(connection_indices: list) -> list:
    """
    :param connection_indices: all returned graphs include these as their connection_indices attribute.
    :return: list of some predefined graphs for query_id = 1

    If the query ids do not start with 1 the query_id needs to be a parameter!
    """

    new_graphs = [
        Graph(graph_type='Preset',
              preset='barchart_run_drill',
              query_aggregate='Mean',
              query_id=1,
              query_index=[0],
              type='timer',
              connection_indices=connection_indices),
        Graph(type='timer',
              name='run',
              graph_type='Histogram',
              query_id=1,
              query_index=[0],
              colorby='DBMS',
              connection_indices=connection_indices),
        Graph(type='timer',
              name='run',
              graph_type='Boxplot',
              query_id=1,
              query_index=[0],
              colorby='DBMS',
              connection_indices=connection_indices),
        Graph(type='timer',
              name='run',
              graph_type='Line Chart',
              query_id=1,
              query_index=[0],
              colorby='DBMS',
              xaxis='Query',
              connection_indices=connection_indices),
    ]
    return new_graphs


class Graph:
    """
    Encapsulate methods regarding graph/table creation.
    """

    def __init__(self, **kwargs):
        """
        Initialize instance attributes with default values and variable number of named arguments.
        Arguments override default values.
        Valid argument keys are:
        ('gridRow', 'gridColumn', 'connection_ids', 'connection_indices', 'query_id', 'query_index',
        'graph_type', 'type', 'name', 'query_aggregate', 'total_aggregate', 'connection_aggregate',
        'warmup', 'cooldown', 'xaxis', 'colorby', 'boxpoints', 'order', 'annotated', 'preset')
        """

        # default attributes
        attributes = dict(
            gridRow=1,
            gridColumn=12,
            connection_ids=[],
            connection_indices=[],
            query_id=None,
            query_index=[],
            graph_type=None,
            type=None,
            name=None,
            query_aggregate=None,
            total_aggregate=None,
            connection_aggregate=None,
            warmup=0,
            cooldown=0,
            xaxis='Connection',
            colorby=None,
            boxpoints=None,
            order='trace',
            annotated=False,
            preset=''
        )

        valid_keys = ['gridRow', 'gridColumn', 'connection_ids', 'connection_indices', 'query_id', 'query_index',
                      'graph_type', 'type', 'name', 'query_aggregate', 'total_aggregate', 'connection_aggregate',
                      'warmup', 'cooldown', 'xaxis', 'colorby', 'boxpoints', 'order', 'annotated', 'preset']

        # override attributes by given arguments
        for key, value in kwargs.items():
            if key in valid_keys:
                attributes[key] = value
            else:
                raise KeyError('Invalid key for Graph instance.')

        # set instance attributes
        for key, value in attributes.items():
            setattr(self, key, value)

    def calculate_connection_aggregate(self, df: pd.DataFrame, e: inspector.inspector) -> pd.DataFrame:
        """
        Calculate the connection aggregate by calling get_aggregated_by_connection() for given DataFrame.
        Only connections in connection_ids attribute are taken into account.

        :param df: DataFrame to calculate connection aggregate for
        :param e: evaluate object
        :return: DataFrame with aggregated connections
        """

        try:
            connections_by_filter = get_connections_by_filter(self.colorby, e)
        except KeyError:
            connections_by_filter = []

        # Filter the connections_by_filter dictionary depending on connection_ids attribute
        connections_by_filter_filtered = dict()
        if self.connection_ids:
            for key in connections_by_filter:
                filtered = list(filter(lambda x: x in self.connection_ids, connections_by_filter[key]))
                if filtered:
                    connections_by_filter_filtered[key] = filtered
        else:
            connections_by_filter_filtered = connections_by_filter  # keep all connections

        df = e.get_aggregated_by_connection(dataframe=df,
                                            list_connections=connections_by_filter_filtered,
                                            connection_aggregate=self.connection_aggregate)
        return df

    def calculate_df(self, e: inspector.inspector) -> pd.DataFrame:
        """
        Calculate DataFrame depending on instance attributes by calling matching inspector method.

        :param e: evaluate object
        :return: Result DataFrame
        """
        if self.name is None or self.type is None:
            raise PreventUpdate

        config = dict(type=self.type,
                      name=self.name,
                      warmup=int(self.warmup),
                      cooldown=int(self.cooldown),
                      dbms_filter=self.connection_ids)

        if self.query_id is None:  # no specific query selected

            config['query_aggregate'] = self.query_aggregate

            if self.query_aggregate is None and self.total_aggregate is None:
                # raw data not supported yet
                raise PreventUpdate

            elif self.total_aggregate is None:
                df = e.get_aggregated_query_statistics(**config)

            else:
                config['total_aggregate'] = self.total_aggregate
                df = e.get_aggregated_experiment_statistics(**config)

        else:  # single query selected
            df, df2 = e.get_measures_and_statistics(int(self.query_id), **config)

            if self.query_aggregate is not None:
                df = df2[[self.query_aggregate]]

            if self.total_aggregate is not None:
                raise PreventUpdate  # TODO handle this case

        if self.connection_aggregate is not None:
            df = self.calculate_connection_aggregate(df, e)

        else:
            # sort df like in list_connections
            df = sort_df_by_list(df, e.get_experiment_list_connections())

        return df

    def construct_title(self) -> str:
        """
        Construct title for a graph/table depending on instance attributes.
        """

        preset_to_unit = {'heatmap_errors': 'bool',
                          'heatmap_warnings': 'bool',
                          'heatmap_total_time': 'ms',
                          'barchart_ingestion_time': 's',
                          'heatmap_result_set_size': ''}

        if self.graph_type == 'Preset' and self.preset in preset_to_unit:

            title = f'{self.preset}'

            unit = preset_to_unit.get(self.preset, '')

            if unit:
                title += f' [{unit}]'

            if self.preset in ['heatmap_errors', 'heatmap_warnings', 'heatmap_result_set_size', 'heatmap_total_time']:
                return title

        else:
            if self.type == 'monitoring':
                try:
                    name = monitor.metrics.metrics[self.name].get('title', self.name)
                except KeyError:
                    name = self.name
            else:
                name = self.name

            if self.graph_type == 'Preset' and self.preset == 'barchart_run_drill':
                name = 'run_drilldown'

            title = f'{self.type} - {name}'

            type_to_unit = {'timer': 'ms',
                            'latency': 'ms',
                            'throughput': 'Hz',
                            'monitoring': ''
                            }

            unit = type_to_unit.get(self.type)

            aggregates = {self.query_aggregate, self.total_aggregate, self.connection_aggregate}
            if len(aggregates.intersection({'factor', 'cv [%]', 'qcod [%]'})) != 0:
                unit = ''

            if unit:
                title += f' [{unit}]'

        if self.query_id is not None:
            title += f' | Q{self.query_id} '

        title += f' | #connections: {len(self.connection_indices)}'

        title += f' | w: {self.warmup}, c: {self.cooldown}'

        if self.query_aggregate is not None:
            title += f' | run: {self.query_aggregate}'
        if self.total_aggregate is not None:
            title += f' | query: {self.total_aggregate}'
        if self.connection_aggregate is not None:
            title += f' | config: {self.connection_aggregate}'

        return title

    def color_switch(self, axis_no_color: str, df: pd.DataFrame, e: inspector.inspector) -> (dict, pd.DataFrame):
        """
        Returns color dict depending on xaxis, colorby and connection_aggregate.
        It removes duplicate code in render_go_... functions.
        Covers all cases besides:
        "self.xaxis = axis_color, self.colorby is NOT None, connection_aggregate is None"

        :param axis_no_color: the xaxis setting where color it not useful applicable.
        :param df: DataFrame
        :param e: evaluate object
        :return: color dictionary with df index mapped to colors and the DataFrame itself
        """
        if self.xaxis == axis_no_color:
            df = df.T
            color = dict()

        elif self.connection_aggregate is None:
            # self.xaxis = !axis_no_color, self.colorby is None, connection_aggregate is None
            color = get_connection_colors(self.colorby, self.connection_ids, e)

        elif self.colorby is None:
            # self.xaxis = !axis_no_color, self.colorby is None, connection_aggregate is NOT None
            color = dict()

        else:
            # self.xaxis = !axis_no_color, self.colorby is NOT None, connection_aggregate is NOT None
            connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby,
                                                                                                self.connection_ids,
                                                                                                e, inc_lists=True)

            # map filter to color of filter representative (like the case without connection aggregate)
            # color_by_filter
            color = {filter_by_connection[x]: connection_colors[x] for x in legend_connections}

            # keep legend order like the case without connection aggregate
            order = [filter_by_connection[x] for x in legend_connections]
            df = sort_df_by_list(df, order)

        return color, df

    def render_go_boxplot(self, df: pd.DataFrame, e: inspector.inspector) -> go.Figure:
        """
        Create graph based on given DataFrame and instance attributes.

        :return: plotly.go figure object
        """

        fig = go.Figure()

        common = dict(
            boxmean='sd',
            hoverinfo='x+y+text',
            boxpoints=self.boxpoints,
            hovertext=df.columns,
            line_width=1
        )

        if self.boxpoints == "False":
            common.update(dict(boxpoints=False))

        if self.xaxis == 'Connection' and self.colorby is not None and self.connection_aggregate is None:
            # self.xaxis = 'Connection', self.colorby is NOT None, connection_aggregate is None
            connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby,
                                                                                                self.connection_ids,
                                                                                                e, inc_lists=True)
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Box(x=[index] * len(row),
                                     y=row,
                                     name=filter_by_connection[index],
                                     legendgroup=filter_by_connection[index],
                                     line_color=connection_colors[index],
                                     showlegend=index in legend_connections,
                                     **common)
                              )
                fig.update_traces(quartilemethod="inclusive")
        else:
            color, df = self.color_switch('Query', df, e)
            
            tools.anonymize_dbms(df)

            # df may change
            common.update(dict(hovertext=df.columns, showlegend=True))

            for index, row in df.iterrows():
                fig.add_trace(go.Box(y=row,
                                     name=index,
                                     line_color=color.get(index),
                                     **common))
                fig.update_traces(quartilemethod="inclusive")

        #
        if self.xaxis == 'Connection':
            xaxis_title = f'{self.xaxis}'
        else:
            xaxis_title = 'Query' if self.query_id is None else 'run number'

        if len(fig.data) == 1:
            xaxis_title = None
            fig.update_layout(showlegend=False)

        if self.xaxis == 'Connection' and self.colorby is not None:
            legend_title_text = self.colorby
        else:
            legend_title_text = xaxis_title

        fig.update_layout(xaxis_title=xaxis_title,
                          legend_title_text=legend_title_text)
        return fig

    def render_go_histogram(self, df: pd.DataFrame, e: inspector.inspector) -> go.Figure:
        """
        Create graph based on given DataFrame and instance attributes.

        :return: plotly.go figure object
        """

        fig = go.Figure()

        common = dict(
            opacity=0.75,
            hoverinfo='x+y+text',
        )

        if self.xaxis == 'Connection' and self.colorby is not None and self.connection_aggregate is None:
            # self.xaxis = 'Connection', self.colorby is NOT None, connection_aggregate is None

            connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby, self.connection_ids, e,
                                                                                                inc_lists=True)
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Histogram(x=row,
                                           name=filter_by_connection[index],
                                           marker_color=connection_colors[index],
                                           hovertext=index,
                                           legendgroup=filter_by_connection[index],
                                           showlegend=index in legend_connections,
                                           **common))

        else:
            color, df = self.color_switch('Query', df, e)
            
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Histogram(x=row,
                                           name=index,
                                           hovertext=index,
                                           marker_color=color.get(index),
                                           showlegend=True,
                                           **common))


        # xaxis_title = ''
        # if self.total_aggregate is not None:
        #     xaxis_title += f'Total {self.total_aggregate} of '
        # if self.query_aggregate is not None:
        #     xaxis_title += f'Query {self.query_aggregate} of '
        # xaxis_title += f'{self.type} - {self.name}'

        yaxis_title = 'Frequency'

        if self.colorby is not None and self.xaxis == 'Connection':
            legend_title_text = self.colorby
        elif self.xaxis == 'Query' and self.query_id is not None:
            legend_title_text = 'run number'
        else:
            legend_title_text = self.xaxis

        if len(fig.data) == 1:
            fig.update_layout(showlegend=False)

        fig.update_layout(
                          #xaxis_title=xaxis_title,
                          yaxis_title=yaxis_title,
                          legend_title_text=legend_title_text)
        return fig

    def render_go_line_chart(self, df: pd.DataFrame, e: inspector.inspector) -> go.Figure:
        """
        Create graph based on given DataFrame and instance attributes.

        :return: plotly.go figure object
        """

        fig = go.Figure()

        common = dict(hoverinfo='x+y+text', line_width=1)

        if self.xaxis == 'Query' and self.colorby is not None and self.connection_aggregate is None:
            # self.xaxis = 'Query', self.colorby is NOT None, connection_aggregate is None

            connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby, self.connection_ids, e,
                                                                                                inc_lists=True)
            
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Scatter(x=df.columns,
                                         y=row,
                                         name=filter_by_connection[index],
                                         legendgroup=filter_by_connection[index],
                                         line_color=connection_colors[index],
                                         showlegend=index in legend_connections,
                                         hovertext=index,
                                         **common))

        else:
            color, df = self.color_switch('Connection', df, e)
            
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Scatter(x=df.columns,
                                         y=row,
                                         name=index,
                                         line_color=color.get(index),
                                         showlegend=True,
                                         hovertext=index,
                                         **common))


        xaxis_title = ''

        # yaxis_title = ''
        # if self.total_aggregate is not None:
        #     yaxis_title += f'Total {self.total_aggregate} of '
        # if self.query_aggregate is not None:
        #     yaxis_title += f'Query {self.query_aggregate} of '
        # yaxis_title += f'{self.type} - {self.name}'

        if self.xaxis == 'Query':
            if self.colorby is not None:
                legend_title_text = self.colorby
            else:
                legend_title_text = 'Connection'

            if self.query_id is not None:
                xaxis_title = 'run number'

        else:  # graph_xaxis == 'Connection'
            if self.query_id is not None:
                legend_title_text = 'run number'
            else:
                legend_title_text = 'Query'

        if len(fig.data) == 1:
            fig.update_layout(showlegend=False)

        fig.update_layout(xaxis_title=xaxis_title,
                          # yaxis_title=yaxis_title,
                          legend_title_text=legend_title_text)
        return fig

    def render_go_bar_chart(self, df: pd.DataFrame, e: inspector.inspector) -> go.Figure:
        """
        Create graph based on given DataFrame and instance attributes.

        :return: plotly.go figure object
        """
        fig = go.Figure()

        if self.xaxis == 'Query' and self.colorby is not None and self.connection_aggregate is None:
            # self.xaxis = 'Query', self.colorby is NOT None, connection_aggregate is None

            connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby, self.connection_ids, e,
                                                                                                inc_lists=True)
            
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Bar(x=df.columns,
                                     y=row.values,
                                     name=filter_by_connection[index],
                                     legendgroup=filter_by_connection[index],
                                     marker_color=connection_colors[index],
                                     showlegend=index in legend_connections,
                                     hoverinfo='x+y+text',
                                     hovertext=index))

            legend_title_text = self.colorby

        else:
            color, df = self.color_switch('Connection', df, e)
            
            tools.anonymize_dbms(df)

            for index, row in df.iterrows():
                fig.add_trace(go.Bar(x=df.columns,
                                     y=row.values,
                                     name=index,
                                     marker_color=color.get(index),
                                     showlegend=True,
                                     hoverinfo='x+y+text',
                                     hovertext=index))

        # yaxis_title = ''
        # if self.total_aggregate is not None:
        #     yaxis_title += f'Total {self.total_aggregate} of '
        # if self.query_aggregate is not None:
        #     yaxis_title += f'Query {self.query_aggregate} of '
        #
        # yaxis_title += f'{self.type} - {self.name}'

        xaxis_title = self.xaxis

        fig.update_layout(xaxis={'categoryorder': self.order})

        fig.update_layout(xaxis_title=xaxis_title,
                          #yaxis_title=yaxis_title,
                          #legend_title_text=legend_title_text,
                          barmode='group')

        return fig

    def render_go_heatmap(self, df: pd.DataFrame, text: str = None) -> go.Figure:
        """
        Create graph based on given DataFrame and instance attributes.
        :param df:
        :param text: optional hover text to show in go.Heatmap
        :return: plotly.go figure object
        """

        tools.anonymize_dbms(df)

        # manual rotate
        if self.xaxis == 'Connection':
            df = df.T
            if text is not None:
                text = text.T

        # workaround for old graphs which do not have annotated attribute
        try:
            logging.debug(self.annotated)
        except AttributeError:
            self.annotated = False

        if self.annotated:
            df_round = df.round(decimals=3)
            
            ff_fig = ff.create_annotated_heatmap(
                    x=[str(x) for x in list(df_round.columns)],
                    y=[str(x) for x in list(df_round.index)],
                    z=df_round.values.tolist(),
                    showscale=True)

            fig = go.Figure(ff_fig)
            fig.layout.xaxis.side = 'bottom'
            fig.layout.xaxis.type = 'category'
            fig.layout.yaxis.type = 'category'

        else:
            fig = go.Figure(
                data=go.Heatmap(
                    x=[str(x) for x in list(df.columns)],
                    y=[str(x) for x in list(df.index)],
                    z=df.values.tolist(),
                    hoverongaps=False,
                    text=text
                )
            )
            fig.layout.xaxis.type = 'category'
            fig.layout.yaxis.type = 'category'

        return fig

    def render_go_preset(self, e: inspector.inspector) -> (go.Figure, pd.DataFrame):
        """
        Render preset figure based on instance attributes.

        :param e: evaluate object
        :return: preset figure and the underlying DataFrame
        """
        if 'heatmap' in self.preset:
            df_text = None

            if self.preset in ['heatmap_result_set_size', 'heatmap_total_time']:

                if self.preset == 'heatmap_result_set_size':
                    # evaluate.get_total_resultsize()
                    df = e.get_total_resultsize_normalized()

                elif self.preset == 'heatmap_total_time':
                    df = e.get_total_times(self.connection_ids)
                    #e.get_total_times_normalized()
                    #e.get_total_times_relative()

                if self.query_id:
                    df = df[[f'Q{self.query_id}']]
                if self.connection_ids:
                    df = df[df.index.isin(self.connection_ids)]

                if self.connection_aggregate is not None:
                    df = self.calculate_connection_aggregate(df, e)

            elif self.preset in ['heatmap_errors', 'heatmap_warnings']:

                self.annotated = False

                if self.preset == 'heatmap_errors':
                    df = e.get_total_errors().replace({False: 0, True: 1})

                    if self.query_id:
                        df = df[[f'Q{self.query_id}']]

                    if self.connection_ids:
                        df = df[df.index.isin(self.connection_ids)]

                    df_text = df.replace({0: '', 1: 'error'})

                    for index, row in df_text.iterrows():
                        for column, value in row.items():
                            if value:
                                try:
                                    numQuery = int(column.replace('Q', ''))
                                    error = e.get_error(numQuery=numQuery, connection=index)
                                except:
                                    error = 'ERROR'

                                df_text.at[index, column] = error


                elif self.preset == 'heatmap_warnings':
                    df = e.get_total_warnings()#.replace({False: 0, True: 1})

                    if self.query_id:
                        df = df[[f'Q{self.query_id}']]

                    if self.connection_ids:
                        df = df[df.index.isin(self.connection_ids)]

                    df_text = df.replace({False: '', True: 'warning'})
                    df = df.replace({False: 0, True: 1})

                    for index, row in df_text.iterrows():
                        for column, value in row.items():
                            if value:
                                try:
                                    numQuery = int(column.replace('Q', ''))
                                    warning = e.get_warning(numQuery=numQuery, connection=index)
                                except:
                                    warning = 'WARNING'

                                df_text.at[index, column] = warning


            elif self.preset == 'heatmap_latency_run':

                self.type = 'latency'
                self.name = 'run'
                df = self.calculate_df(e=e)

            elif self.preset == 'heatmap_throughput_run':

                self.type = 'throughput'
                self.name = 'run'
                df = self.calculate_df(e=e)

            elif self.preset == 'heatmap_timer_run_factor':

                self.type = 'timer'
                self.name = 'run'
                df = self.calculate_df(e=e)

            fig = self.render_go_heatmap(df, df_text)

        elif self.preset == 'barchart_run_drill':

            if self.total_aggregate is None and self.query_id is None:
                raise PreventUpdate

            timer = ['execution', 'datatransfer', 'connection']
            df = pd.DataFrame()

            if self.query_aggregate is None:
                raise PreventUpdate

            if self.type is None or self.type == 'monitoring':
                raise PreventUpdate

            self.total_aggregate = None if self.query_id is not None else self.total_aggregate
            self.xaxis = 'Connection'

            for name in timer:
                self.name = name
                try:
                    t = self.calculate_df(e=e)
                except KeyError:
                    continue

                if self.query_id is not None:
                    t = t.rename(columns={self.query_aggregate: f'total_timer_{name}'})

                if df.empty:
                    df = t
                else:
                    df = df.join(t)


            logging.debug(df)

            # fig = go.Figure()
            # for index, column in df.iteritems():
            #     fig.add_trace(go.Bar(x=df.index,
            #                          y=column.to_list(),
            #                          name=index,
            #                          hovertext=column.name,
            #                          hoverinfo='x+y+text'))

            fig = self.render_go_bar_chart(df, e)

            fig.update_layout(barmode='stack',
                              xaxis_title='Connection',
                              yaxis_title='',
                              legend_title_text='Timer')

        elif self.preset == 'barchart_ingestion_time':

            connections = e.get_experiment_list_connections()

            if self.connection_ids:
                connections = list(filter(lambda x: x in self.connection_ids, connections))

            load_ms = list(map(lambda x: e.get_experiment_connection_properties(x)['times']['load_ms'], connections))
            df = pd.DataFrame(load_ms, index=connections, columns=['load_ms'])
            df = df / 1000
            df = df.round(decimals=3)
            df.rename_axis('DBMS')
            df.index.name = 'DBMS'

            tools.anonymize_dbms(df)

            if self.colorby is not None and self.connection_aggregate is None:
                connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby,
                                                                                                    self.connection_ids, e,
                                                                                                    inc_lists=True)
                name = lambda x: filter_by_connection[x]
                marker_color = lambda x: connection_colors[x]
                showlegend = lambda x: x in legend_connections
                legendgroup = lambda x: filter_by_connection[x]
            else:

                name = lambda x: x
                showlegend = lambda x: False
                legendgroup = lambda x: ""

                # colorby is NOT None, connection_aggregate is NOT None
                if self.colorby is not None:
                    df = self.calculate_connection_aggregate(df, e)
                    connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby,
                                                                                                        self.connection_ids, e,
                                                                                                        inc_lists=True)
                    color_by_filter = {filter_by_connection[x]: connection_colors[x] for x in legend_connections}
                    marker_color = lambda x: color_by_filter[x]

                # colorby is None, connection_aggregate is None
                elif self.connection_aggregate is None:
                    connection_colors = get_connection_colors(self.colorby, self.connection_ids, e)
                    marker_color = lambda x: connection_colors[x]

                # colorby is NONE, connection_aggregate is NOT None
                else:
                    df = self.calculate_connection_aggregate(df, e)
                    marker_color = lambda x: None

            fig = go.Figure()
            # Not using get_bar_chart because color is used different here
            for index, row in df.iterrows():
                fig.add_trace(go.Bar(x=[index],
                                     y=row.values,
                                     name=name(index),
                                     marker_color=marker_color(index),
                                     showlegend=showlegend(index),
                                     hoverinfo='x+y',
                                     legendgroup=legendgroup(index)))

            fig.update_layout(xaxis_title='Connection', yaxis_title='')

        return fig, df

    def render_go(self, e: inspector.inspector, index: int) -> (html.Div, pd.DataFrame):
        """
        # Call the according render_go_... function based on an instance graph type.

        :param e: evaluate object
        :param index: the index of the graphs id
        :return: Div including the graph and the underlying DataFrame
        """

        if self.graph_type is None:
            raise PreventUpdate

        elif self.graph_type == 'Preset':
            if self.preset == '':
                raise PreventUpdate

            fig, df = self.render_go_preset(e=e)

        else:

            df = self.calculate_df(e=e)

            if self.graph_type == 'Heatmap':
                fig = self.render_go_heatmap(df)

            elif self.graph_type == 'Line Chart':
                fig = self.render_go_line_chart(df, e)

            elif self.graph_type == 'Histogram':
                fig = self.render_go_histogram(df, e)

            elif self.graph_type == 'Boxplot':
                fig = self.render_go_boxplot(df, e)

            elif self.graph_type == 'Bar Chart':
                fig = self.render_go_bar_chart(df, e)


        title = self.construct_title()
        df.name = title

        fig.update_layout(title=title,
                          autosize=True,
                          # height=600,
                          margin=dict(
                              l=50,
                              r=50,
                              b=100,
                              t=50,
                              pad=4
                          ),
                          )

        graph = dcc.Graph(figure=fig,
                          responsive=True,
                          style=dict(height='100%'),
                          id=dict(type='graph', index=index))

        return graph, df

    def render_table(self, e: inspector.inspector, index: int) -> (html.Div, pd.DataFrame):
        """
        Render the table widget based on instance attributes.

        :param e: evaluate object
        :param index: the tables id index
        :return: Div including the table and the underlying DataFrame
        """

        df = self.calculate_df(e)

        tools.anonymize_dbms(df)

        style_data_conditional = []
        style_header_conditional = []
        tooltip_data = []

        if self.connection_aggregate is not None and self.colorby is not None:
            df.index.name = self.colorby

        # check if colorby is applicable
        if (self.xaxis == 'Query' or self.graph_type == 'Table Measures') and \
                not(self.colorby is None and self.connection_aggregate is not None):

            if self.colorby is None:
                # connection_aggregate is None
                connection_colors = get_connection_colors(self.colorby, self.connection_ids, e)

            else:
                connection_colors, legend_connections, filter_by_connection = get_connection_colors(self.colorby,
                                                                                                    self.connection_ids,
                                                                                                    e, inc_lists=True)
                # add tooltip of filter group only in this certain case
                if self.connection_aggregate is None:
                    if self.xaxis == 'Query':
                        for i, item in enumerate(list(df.index)):
                            tt_value = str(filter_by_connection[item])
                            if len(tt_value) == 0:
                                tt_value = 'None'
                            tooltip = {df.index.name: {'type': 'text', 'value': tt_value, 'delay': 50, 'duration': 36000}}
                            tooltip_data.append(tooltip)

                else:
                    # get colors by filter class for connection aggregate
                    connection_colors = {filter_by_connection[x]: connection_colors[x] for x in legend_connections}

            # apply colors to first column
            if self.xaxis == 'Query':
                for i, item in enumerate(list(df.index)):
                    style = {
                        'if': {
                            'column_id': df.index.name,
                            'row_index': i,
                        },
                        'backgroundColor': connection_colors[item],
                        'color': 'white'
                    }
                    style_data_conditional.append(style)

            # apply colors to header row
            else:
                for i, item in enumerate(list(df.index)):
                    style = {
                        'if': {
                            'column_id': item,
                        },
                        'backgroundColor': connection_colors[item],
                        'color': 'white'
                    }
                    style_header_conditional.append(style)

        df = df.rename(columns={i: str(i) for i in df.columns})

        if self.xaxis == 'Connection':
            df = df.T
            df.index.name = 'Query'

        if self.graph_type == 'Table Statistics':
            df = evaluator.addStatistics(df, drop_nan=False, drop_measures=True)

        df_return = df  # save dataframe to return before manipulation
        df = evaluator.addStatistics(df.T, drop_nan=False).T
        df = df.round(decimals=3)  # todo make optional
        df = df.reset_index()

        table = dash_table.DataTable(
            id=dict(type='graph', index=index),
            style_cell={'textAlign': 'left'},
            # fixed_rows={'headers': True},
            # fixed_columns={'headers': True, 'data': 1},
            style_table={'overflow': 'auto',
                         'maxWidth': '100%',
                         'maxHeight': 'calc(100% - 45px)'},
            columns=[{'name': i, 'id': i} for i in df.columns],
            data=df.to_dict('records'),
            style_data_conditional=style_data_conditional,
            style_header_conditional=style_header_conditional,
            tooltip_data=tooltip_data,
            page_action='none',
        )

        title = self.construct_title()
        df_return.name = title
        widget = html.Div([html.Span(title), table], style=dict(height='100%'))
        return widget, df_return

    def render(self, code: str, session_id: str, index: int) -> (html.Div, pd.DataFrame):
        """
        # Call the according render function based on an instance graph type.

        :param code: experiment code
        :param session_id: the users session id
        :param index: the index of the graphs id
        :return: Div including the graph and the underlying DataFrame
        """

        # get evaluate object
        e = load_experiment(code)

        if 'Table' in (self.graph_type or ''):
            graph, df = self.render_table(e, index)
        else:
            graph, df = self.render_go(e, index)

        return graph, df


########################################################################################
#                                                                                      #
#                                                                                      #
#                                       CALLBACKS                                      #
#                                                                                      #
#                                                                                      #
########################################################################################


########################################################################################
#                                                                                      #
#                                SELECT EXPERIMENT OVERLAY                             #
#                                                                                      #
########################################################################################


@app.callback(
    Output('table_experiment_preview', 'selected_rows'),
    [Input('store_loaded_experiment', 'modified_timestamp')],
    [State('table_experiment_preview', 'data'),
     State('table_experiment_preview', 'selected_rows'),
     State('store_loaded_experiment', 'data')]
)
def load_code_from_session(modified_timestamp, exp_table_data, exp_selected_rows, code):
    """
    Load experiment code from session storage and select corresponding row in the experiment table
    """
    if code is None:
        raise PreventUpdate

    if code == -1:
        return []

    else:
        # Prevent update if exp_selected_rows already set
        if exp_selected_rows is None or exp_selected_rows == []:
            for index, row in enumerate(exp_table_data):
                if row['code'] == code:
                    return [index]

    raise PreventUpdate


@app.callback(
    Output('store_loaded_experiment', 'data'),
    [Input('table_experiment_preview', 'selected_rows')],
    [State('table_experiment_preview', 'data'),
     State('store_session_id', 'data')]
)
def load_selected_experiment(selected_rows, table_data, session_id):
    """
    Load experiment into global variable 'evaluate' and store code in dcc.Store #todo adjust for cache
    """
    if selected_rows is None or selected_rows == []:
        raise PreventUpdate

    code = table_data[selected_rows[0]]['id']

    e = load_experiment(code)
    return code


@app.callback(
    Output('table_experiment_preview', 'style_data_conditional'),
    [Input('table_experiment_preview', 'selected_rows')],
)
def highlight_selected_experiment(selected_rows):
    """
    Highlight selected row in experiment preview table
    """
    if selected_rows is None or selected_rows == []:
        raise PreventUpdate

    style_data_conditional = [{
        "if": {"row_index": selected_rows[0]},
        "backgroundColor": "#3D9970",
        'color': 'white'
    }]
    return style_data_conditional


@app.callback(
    Output('experiment_overlay', 'style'),
    [Input('btn_toggle_experiment_overlay', 'n_clicks'),
     Input('experiment_overlay', 'n_clicks'),
     Input('experiment_overlay_content', 'n_clicks'),
     Input('table_experiment_preview', 'selected_rows')],
)
def toggle_select_experiment_overlay(nc1, nc2, nc3, selected_rows):
    """
    open/close the select experiment overlay on button click and app start, depending on if experiment is selected.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    if 'table_experiment_preview.selected_rows' in callback_context:
        if selected_rows:
            return dict(height='0%')
        else:
            return dict(height='100%')

    elif callback_context == ['btn_toggle_experiment_overlay.n_clicks']:
        return dict(height='100%')

    elif callback_context == ['experiment_overlay.n_clicks']:
        return dict(height='0%')

    raise PreventUpdate


@app.callback(
    Output('div_workflow_name', 'children'),
    [Input('store_loaded_experiment', 'data')],
    [State('store_session_id', 'data')]
)
def show_workflow_name(code, session_id):
    """
    Show workflow name in navbar after an experiment is loaded.
    """
    e = load_experiment(code)
    workload_properties = e.get_experiment_workload_properties()
    return workload_properties.get('name', '')


########################################################################################
#                                                                                      #
#                                       FILTER SIDE MENU                               #
#                                                                                      #
########################################################################################

# # # QUERIES # # #

@app.callback(
    [Output('table_query', 'columns'),
     Output('table_query', 'data')],
    [Input('store_loaded_experiment', 'data')],
    [State('store_session_id', 'data')]
)
def render_query_table(code, session_id):
    """
    Render the query table after an experiment is selected.
    """
    if code is None:
        raise PreventUpdate

    # load evaluate object
    e = load_experiment(code)

    # gather query data and create a DataFrame with the columns numQuery and title
    list_queries = e.get_experiment_list_queries()
    titles = [e.get_experiment_query_properties(numQuery)['title'] for numQuery in list_queries]
    df = pd.DataFrame(list(zip(list_queries, titles)), columns=['numQuery', 'title'])

    # add id columns as datatable row ids
    df['id'] = df['numQuery']

    return [{'name': i, "id": i} for i in df.drop(columns=['id']).columns], df.to_dict('records')


@app.callback(
    Output('table_query', 'selected_rows'),
    [Input('btn_deselect_query', 'n_clicks'),
     Input('store_active_graph', 'data')],
    [State('store_loaded_experiment', 'data'),
     State('store_dashboard_config', 'data'),
     State('signal_add_graph', 'data')]
)
def filter_query(btn_deselect_query, active_graph, code, store_dashboard_config, signal_add_graph):
    """
    Adjust selected rows in query table when deselect button is clicked or active graph is changed.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('** filter_query **')
    logging.debug(callback_context)

    # active graph changed
    if callback_context == ['store_active_graph.data']:
        if active_graph:
            if len(active_graph) > 1:  # multiple active graphs -> select none
                combined_dict = {**signal_add_graph.get('add', dict()), **store_dashboard_config.get(code, dict())}
                selected_queries = [combined_dict[str(index)]['query_index'] for index in active_graph]
                if all(el == selected_queries[0] for el in selected_queries):
                    return selected_queries[0]
                else:
                    return []
            else:  # single active graph -> load query selection from dashboard config or signal_add_graph
                combined_dict = {**signal_add_graph.get('add', dict()), **store_dashboard_config.get(code, dict())}
                return combined_dict[str(active_graph[0])]['query_index']

    # deselect button clicked
    elif callback_context == ['btn_deselect_query.n_clicks']:
        return []

    raise PreventUpdate


# # # CONNECTIONS # # #

@app.callback(
    [Output('table_dbms', 'columns'),
     Output('table_dbms', 'data')],
    [Input('store_loaded_experiment', 'data')],
    [State('store_session_id', 'data')]
)
def render_connection_table(code, session_id):
    """
    Render the connection table after an experiment is selected.
    """
    if code is None:
        raise PreventUpdate

    # load evaluate object
    e = load_experiment(code)

    # example for tooltip_data
    # tooltip_data = [{'connection': {'type': 'markdown', 'value': 'some tooltip', 'delay': 50, 'duration': 300000}}
    #                 for c in e.get_experiment_list_connections()]

    columns = [{'name': 'connection', "id": 'connection'}]
    data = [{'connection': tools.anonymize_dbms(x), 'id': x} for x in e.get_experiment_list_connections()]
    return columns, data


@app.callback(
    [Output('dropdown_dbms', 'options'),
     Output('dropdown_node', 'options'),
     Output('dropdown_client', 'options'),
     Output('dropdown_gpu', 'options'),
     Output('dropdown_cpu', 'options'),
     Output('dropdown_dbms', 'optionHeight'),
     Output('dropdown_node', 'optionHeight'),
     Output('dropdown_client', 'optionHeight'),
     Output('dropdown_gpu', 'optionHeight'),
     Output('dropdown_cpu', 'optionHeight')],
    [Input('store_loaded_experiment', 'data')],
    [State('store_session_id', 'data')]
)
def set_connection_table_filter_options(code, session_id):
    """
    Fill filter dropdowns with available options and increase optionHeight (default 35) if necessary.
    """

    if code is None:
        raise PreventUpdate

    e = load_experiment(code)

    lists = [e.get_experiment_list_dbms(),
             e.get_experiment_list_nodes(),
             e.get_experiment_list_connections_by_connectionmanagement('numProcesses'),
             e.get_experiment_list_connections_by_hostsystem('GPU'),
             e.get_experiment_list_connections_by_hostsystem('CPU')]

    # generate options for all lists
    options = tuple(map(lambda l: [{'label': i if i != '' else '*None*', 'value': i} for i in l], lists))

    # generate optionsHeight for all lists
    options_heights = tuple(map(lambda l: 35 + int(max(map(len, map(str, l))) / 30) * 15, lists))

    return options + options_heights


# Adjust selected rows in dbms / connections table depending on filters
@app.callback(
    [Output('table_dbms', 'selected_rows'),
     Output('dropdown_dbms', 'value'),
     Output('dropdown_node', 'value'),
     Output('dropdown_client', 'value'),
     Output('dropdown_gpu', 'value'),
     Output('dropdown_cpu', 'value')],
    [Input('btn_apply_filter', 'n_clicks'),
     Input('btn_deselect_all_connections', 'n_clicks'),
     Input('btn_select_all_connections', 'n_clicks'),
     Input('store_active_graph', 'data')],
    [State('store_dashboard_config', 'data'),
     State('dropdown_dbms', 'value'),
     State('dropdown_node', 'value'),
     State('dropdown_client', 'value'),
     State('dropdown_gpu', 'value'),
     State('dropdown_cpu', 'value'),
     State('table_dbms', 'data'),
     State('table_dbms', 'selected_rows'),
     State('store_loaded_experiment', 'data'),
     State('store_session_id', 'data'),
     State('signal_add_graph', 'data')]
)
def filter_connections(nc1, nc2, nc3, active_graph, store_dashboard_config, dbms, node, client, gpu, cpu,
                       table_data, table_selected_rows, code, session_id, signal_add_graph):

    """
    Adjust selected rows in the connection table on user input or when active graph is changed.
    User input may be the apply filter button or the select/deselect all button.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('** filter_connections**')
    logging.debug(callback_context)

    # no actual trigger -> prevent update
    if callback_context == ['.']:
        raise PreventUpdate

    # active graphs changed -> load connections according to them
    elif callback_context == ['store_active_graph.data']:
        if active_graph:
            combined_dict = {**signal_add_graph.get('add', dict()), **store_dashboard_config.get(code, dict())}
            if len(active_graph) > 1:  # multiple active graphs -> select all
                selected_connections = [combined_dict[str(index)]['connection_indices'] for index in active_graph]
                if all(el == selected_connections[0] for el in selected_connections):
                    return [selected_connections[0]] + [no_update] * 5
                else:
                    return [[]] + [no_update] * 5

            else:  # single active graph -> load connection selection from config or signal_add_graph
                return [combined_dict[str(active_graph[0])]['connection_indices']] + [no_update] * 5

    # select all button clicked -> select all and don't update filter dropdowns
    elif callback_context == ['btn_select_all_connections.n_clicks']:
        return [list(range(len(table_data)))] + [no_update] * 5

    # deselect all button clicked -> deselect all and don't update filter dropdowns
    elif callback_context == ['btn_deselect_all_connections.n_clicks']:
        return [[]] + [no_update] * 5

    # apply filter button clicked -> apply chosen filters to connections and reset filter dropdowns
    elif callback_context == ['btn_apply_filter.n_clicks']:

        e = load_experiment(code)

        # all connections
        connections_filtered = set(e.get_experiment_list_connections())

        # create dict with filter name and value pairs
        dict_filter_values = {'DBMS': dbms, 'Node': node, 'Client': client, 'GPU': gpu, 'CPU': cpu}

        # filter connections
        for filter_name, filter_value in dict_filter_values.items():
            if filter_value is not None and filter_value != []:
                temp_set = set()
                temp_filter_lists = get_connections_by_filter(filter_name, e)
                for filter_ in filter_value:
                    # union of temp_sets
                    temp_set = temp_set.union(temp_filter_lists[filter_])
                # intersect connections_filtered with the union of temp_sets
                connections_filtered = connections_filtered.intersection(temp_set)

        # convert set to list
        connections_filtered = list(connections_filtered)

        # Convert ids to indices
        selected_rows = []
        for index, row in enumerate(table_data):
            if row['id'] in connections_filtered:
                selected_rows.append(index)

        # intersection of current selected rows and filter
        result = list(set(table_selected_rows or []).intersection(set(selected_rows)))
        return [result] + [None] * 5  # reset filter dropdowns after apply filter

    raise PreventUpdate


########################################################################################
#                                                                                      #
#                                       SETTINGS SIDE MENU                             #
#                                                                                      #
########################################################################################


@app.callback(
    Output('dd_name', 'options'),
    [Input('dd_type', 'value')]
)
def fill_name_dropdown(value):
    """
    Adjust the name dropdown options depending on type selection.
    """
    if value is None:
        raise PreventUpdate
    elif value == 'monitoring':
        temp = list(monitor.metrics.metrics.keys())  # todo adjust for cache
    else:
        temp = ['connection', 'execution', 'datatransfer', 'run', 'session']

    return [{'label': x, 'value': x} for x in temp]


@app.callback(
    [Output('heading_settings', 'style'),  # 0
     Output('label_type', 'style'),
     Output('dd_type', 'style'),
     Output('label_name', 'style'),
     Output('dd_name', 'style'),
     Output('label_query_aggregate', 'style'),  # 5
     Output('dd_query_aggregate', 'style'),
     Output('label_total_aggregate', 'style'),
     Output('dd_total_aggregate', 'style'),
     Output('label_graph_type', 'style'),
     Output('dd_graph_type', 'style'),  # 10
     Output('div_warmup', 'style'),
     Output('div_cooldown', 'style'),
     Output('label_graph_xaxis', 'style'),
     Output('dd_graph_xaxis', 'style'),
     Output('label_graph_colorby', 'style'),  # 15
     Output('dd_graph_colorby', 'style'),
     Output('label_boxpoints', 'style'),
     Output('dd_boxpoints', 'style'),
     Output('label_order', 'style'),
     Output('dd_order', 'style'),  # 20
     Output('radio_preset_graph', 'style'),
     Output('label_connection_aggregate', 'style'),
     Output('dd_connection_aggregate', 'style'),
     Output('checklist_annotate', 'style'),
     Output('div_input_gridColumn_all', 'style'),
     Output('div_input_gridRow_all', 'style')],
    [Input('store_active_graph', 'data'),
     Input('store_dashboard_config', 'data')],
    [State('store_loaded_experiment', 'data')]
)
def toggle_settings_visibility(active_graph, store_dashboard_config, code):
    """
    Show/hide settings elements depending on graph type.
    """
    logging.debug('** toggle_settings_visible **')

    # assign output position to variables
    dd_type = {1, 2}
    dd_name = {3, 4}
    dd_query_aggregate = {5, 6}
    dd_total_aggregate = {7, 8}
    dd_graph_type = {9, 10}
    warmup_cooldown = {11, 12}
    dd_graph_xaxis = {13, 14}
    dd_graph_colorby = {15, 16}
    dd_boxpoints = {17, 18}
    dd_order = {19, 20}
    radio_preset_graph = {21}
    dd_connection_aggregate = {22, 23}
    checklist_annotate = {24}
    div_input_grid_column_row = {25, 26}

    to_show = set()

    if len(active_graph) > 1:  # multiple graphs selected
        to_show = dd_type | dd_name | dd_query_aggregate | dd_total_aggregate | dd_connection_aggregate | \
                  warmup_cooldown | dd_graph_colorby | div_input_grid_column_row

    elif active_graph:

        graph = store_dashboard_config.get(code, dict()).get(str(active_graph[0]))

        if graph:
            graph_type = graph.get('graph_type')

            # preset graph
            if graph_type == 'Preset':

                # show preset radios and graph type as default for all preset graphs
                to_show = radio_preset_graph | dd_graph_type

                if graph['preset'] == 'barchart_run_drill':
                    to_show |= dd_type | dd_query_aggregate | dd_total_aggregate | dd_connection_aggregate | \
                               warmup_cooldown | dd_graph_colorby

                    if graph['query_id'] is not None:
                        to_show -= dd_total_aggregate

                elif graph['preset'] in ['heatmap_latency_run', 'heatmap_throughput_run', 'heatmap_timer_run_factor']:
                    to_show |= dd_query_aggregate | dd_total_aggregate | dd_connection_aggregate | \
                               warmup_cooldown | dd_graph_colorby

                elif graph['preset'] == 'barchart_ingestion_time':
                    to_show |= dd_connection_aggregate | dd_graph_colorby

                elif graph['preset'] == 'heatmap_total_time':
                    to_show |= dd_connection_aggregate | dd_graph_colorby

                # add xaxis dropdown and annotate option for all heatmaps except heatmap_errors and heatmap_warnings
                if 'heatmap' in graph['preset']:
                    to_show |= dd_graph_xaxis | checklist_annotate
                    if graph['preset'] in ['heatmap_errors', 'heatmap_warnings']:
                        to_show -= checklist_annotate

                # add order dropdown for all barcharts
                if 'barchart' in graph['preset']:
                    to_show |= dd_order

            # standard graph
            else:
                # default for standard graphs
                to_show = set(range(0, 17)) | dd_connection_aggregate

                if graph_type == 'Boxplot':
                    to_show |= dd_boxpoints

                elif graph_type == 'Bar Chart':
                    to_show |= dd_order

                elif graph_type == 'Heatmap':
                    to_show |= checklist_annotate

    # prepare returned styles
    styles = [dict(display='none')] * 27
    for x in to_show:
        styles[x] = dict()

    return styles


@app.callback(
    [Output('radio_preset_graph', 'value'),
     Output('signal_pre_settings', 'data')],
    [Input('store_active_graph', 'data')],
    [State('store_dashboard_config', 'data'),
     State('store_loaded_experiment', 'data'),
     State('signal_add_graph', 'data')]
)
def pre_set_settings(active_graph, store_dashboard_config, code, signal_add_graph):
    """
    When the active graph changes the radio_preset_graph is adjusted accordingly.
    A signal is sent to set_settings to show that radio_preset_graph changes come from here (not user input).
    """
    logging.debug('\n** pre_set_settings **')

    if active_graph:
        if len(active_graph) == 1:

            # load preset value from config or signal_add_graph
            combined_dict = {**signal_add_graph.get('add', dict()), **store_dashboard_config.get(code, dict())}
            preset = combined_dict[str(active_graph[0])].get('preset', '')
            if len(preset) > 0:
                return preset, 1

    return None, 1


@app.callback(
    [Output('dd_type', 'value'),
     Output('dd_name', 'value'),
     Output('dd_query_aggregate', 'value'),
     Output('dd_total_aggregate', 'value'),
     Output('dd_connection_aggregate', 'value'),
     Output('dd_graph_type', 'value'),
     Output('slider_warmup', 'value'),
     Output('slider_cooldown', 'value'),
     Output('dd_graph_xaxis', 'value'),
     Output('dd_graph_colorby', 'value'),
     Output('dd_boxpoints', 'value'),
     Output('dd_order', 'value'),
     Output('checklist_annotate', 'value')],
    [Input('radio_preset_graph', 'value'),
     Input('signal_pre_settings', 'data')],
    [State('store_dashboard_config', 'data'),
     State('store_loaded_experiment', 'data'),
     State('store_active_graph', 'data'),
     State('signal_add_graph', 'data')]
)
def set_settings(radio_preset_graph, signal_pre_settings, store_dashboard_config, code, active_graph, signal_add_graph):
    """
    After radio_preset_graph input adjust other settings according to the active graph(s).
    This callback is triggered in one of two ways:
    1. when the active graph changes it is triggered by pre_set_settings
    2. when the user changes radio_preset_graph manually
    The workaround is necessary for setting radio_preset_graph when the active graph changes.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    logging.debug('\n** set_settings **')
    logging.debug(callback_context)

    # callback triggered by pre_set_settings
    if len(callback_context) == 2:

        if active_graph:
            if len(active_graph) > 1:  # multiple graphs selected -> set settings to None
                return (None,) * 12 + ([],)

            else:
                # active graphs configuration from dashboard store or signal_add_graph
                combined_dict = {**signal_add_graph.get('add', dict()), **store_dashboard_config.get(code, dict())}
                config = combined_dict[str(active_graph[0])]

                # adjust the annotated value
                checklist_annotate = ['annotate'] if config.get('annotated') else []

                # return settings stored in active graphs configuration
                return (config.get('type'), config.get('name'), config.get('query_aggregate'),
                        config.get('total_aggregate'), config.get('connection_aggregate'),
                        config.get('graph_type'), config.get('warmup'),
                        config.get('cooldown'), config.get('xaxis'), config.get('colorby'),
                        config.get('boxpoints'), config.get('order'), checklist_annotate)

    # callback triggered by radio_preset_graph manual change
    elif callback_context == ['radio_preset_graph.value']:

        # default values for preset graphs
        graph_type = 'Preset'
        query_aggregate = None
        total_aggregate = None
        connection_aggregate = None
        colorby = None
        type = no_update

        if radio_preset_graph in ['heatmap_latency_run', 'heatmap_throughput_run', 'heatmap_timer_run_factor']:
            query_aggregate = 'factor'

        elif radio_preset_graph == 'barchart_run_drill':
            query_aggregate = 'Mean'
            total_aggregate = 'Mean'
            type = 'timer'

        # return necessary settings for preset graph
        return (type, no_update, query_aggregate, total_aggregate, connection_aggregate, graph_type,
                no_update, no_update, no_update, colorby, no_update, no_update, no_update)

    raise PreventUpdate


@app.callback(
    Output('signal_update_settings', 'data'),
    [Input('dd_type', 'value'),
     Input('dd_name', 'value'),
     Input('dd_query_aggregate', 'value'),
     Input('dd_total_aggregate', 'value'),
     Input('dd_connection_aggregate', 'value'),
     Input('dd_graph_type', 'value'),
     Input('dd_graph_xaxis', 'value'),
     Input('dd_graph_colorby', 'value'),
     Input('slider_warmup', 'value'),
     Input('slider_cooldown', 'value'),
     Input('dd_boxpoints', 'value'),
     Input('dd_order', 'value'),
     Input('table_dbms', 'selected_rows'),
     Input('table_query', 'selected_rows'),
     Input('checklist_annotate', 'value')],
    [State('table_dbms', 'data'),
     State('table_query', 'data'),
     State('radio_preset_graph', 'value')]
)
def process_settings_input(type, name, query_aggregate, total_aggregate, connection_aggregate, graph_type, graph_xaxis,
                           graph_color, warmup, cooldown, boxpoints, order, connections_selected_rows,
                           query_selected_rows, checklist_annotate, connections_table_data,
                           query_table_data, radio_preset_graph):
    """
    Process settings input and forward it to update_store using signal_update_settings.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('\n** process_settings_input **')
    logging.debug(callback_context)

    # multiple values changed
    # determine if callback was triggered by activate graph or changing preset
    if len(callback_context) > 1 or callback_context == ['.']:

        # if preset radio was changed the callback_context length is under 14 because
        # set_settings updates not all settings
        # when activating a graph all settings are updated
        if len(callback_context) < 14:
            store_data = dict(
                preset=radio_preset_graph,
                graph_type=graph_type,
                query_aggregate=query_aggregate,
                total_aggregate=total_aggregate,
                connection_aggregate=connection_aggregate,
                colorby=graph_color,
                xaxis=graph_xaxis,
                type=type,
            )
        else:
            logging.debug('PreventUpdate')
            raise PreventUpdate

    # single value changed -> change this value signal change this value
    else:
        store_data = dict()

        # update only the value that changed
        if callback_context == ['dd_type.value']:
            # adjust name too when type is monitoring
            if type == 'monitoring' or name not in ['connection', 'execution', 'datatransfer', 'run', 'session', None]:
                store_data['name'] = None
            store_data['type'] = type

        elif callback_context == ['dd_name.value']:
            store_data['name'] = name

        elif callback_context == ['dd_query_aggregate.value']:
            store_data['query_aggregate'] = query_aggregate

        elif callback_context == ['dd_total_aggregate.value']:
            store_data['total_aggregate'] = total_aggregate

        elif callback_context == ['dd_connection_aggregate.value']:
            store_data['connection_aggregate'] = connection_aggregate

        elif callback_context == ['dd_graph_type.value']:
            store_data['graph_type'] = graph_type

        elif callback_context == ['dd_graph_xaxis.value']:
            store_data['xaxis'] = graph_xaxis

        elif callback_context == ['dd_graph_colorby.value']:
            store_data['colorby'] = graph_color

        elif callback_context == ['slider_warmup.value']:
            store_data['warmup'] = warmup

        elif callback_context == ['slider_cooldown.value']:
            store_data['cooldown'] = cooldown

        elif callback_context == ['dd_boxpoints.value']:
            store_data['boxpoints'] = boxpoints

        elif callback_context == ['dd_order.value']:
            store_data['order'] = order

        elif callback_context == ['table_dbms.selected_rows']:
            store_data['connection_ids'] = [connections_table_data[i]['id'] for i in connections_selected_rows]
            store_data['connection_indices'] = connections_selected_rows

        elif callback_context == ['table_query.selected_rows']:
            store_data['query_id'] = None if query_selected_rows is None or query_selected_rows == [] \
                                     else query_table_data[query_selected_rows[0]]['numQuery']
            store_data['query_index'] = query_selected_rows

        elif callback_context == ['checklist_annotate.value']:
            store_data['annotated'] = 'annotate' in checklist_annotate

    return store_data


@app.callback(
    [Output(dict(type='input_gridColumn', index=ALL), 'value'),
     Output(dict(type='input_gridRow', index=ALL), 'value')],
    [Input('input_gridColumn_all', 'value'),
     Input('input_gridRow_all', 'value')],
    [State(dict(type='input_gridColumn', index=ALL), 'id')]
)
def resize_all(grid_column, grid_row, ids):
    """
    Process input for input_gridColumn_all and input_gridRow_all in settings side menu.
    Changes all input_gridColumn or input_gridRow values.
    """
    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    n = len(ids)
    if callback_context == ['input_gridColumn_all.value']:
        return [grid_column] * n, [no_update] * n
    elif callback_context == ['input_gridRow_all.value']:
        return [no_update] * n, [grid_row] * n
    else:
        raise PreventUpdate


########################################################################################
#                                                                                      #
#                                       GRAPH ICONS                                    #
#                                                                                      #
########################################################################################


@app.callback(
    Output('signal_move', 'data'),
    [Input(dict(type='icon_move_up', index=ALL), 'n_clicks'),
     Input(dict(type='icon_move_down', index=ALL), 'n_clicks')],
    [State('signal_update_graphs', 'data'),
     State(dict(type='input_gridRow', index=ALL), 'id'),
     State(dict(type='input_gridColumn', index=ALL), 'id'),
     State('store_dashboard_config', 'data'),
     State('store_loaded_experiment', 'data')]
)
def process_move_input(icon_move_up, icon_move_down, signal_update_graphs, input_grid_row_ids, input_grid_column_ids,
                       store_dashboard_config, code):
    """
    Process move inputs than send signal_move.
    Determine the indices of the elements to swap.
    signal_move is a dict like {index1:index2, index2:index1}
    where index1 and 2 are from the elements to swap.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('\n** process_resize_or_move_input**')
    logging.debug(callback_context)

    # only process if triggered by only one valid input
    if len(callback_context) == 1 and callback_context != ['.']:

        # determine the ids index of element in callback context and the type by extracting it from trigger string name
        trigger = callback_context[0]
        temp = json.loads(trigger[trigger.index('{'):trigger.index('}') + 1])
        index1 = temp['index']
        direction = temp['type'][10:]

        keys = list(map(int, store_dashboard_config[code].keys()))
        keys.sort()
        position = keys.index(index1)
        str_to_int = {'up': 1, 'down': -1}

        x = position + str_to_int[direction]
        if 0 <= x < len(keys):
            index2 = keys[position + str_to_int[direction]]
            return dict([(index1, index2), (index2, index1)])

    raise PreventUpdate


@app.callback(
    Output('signal_resize', 'data'),
    [Input(dict(type='input_gridRow', index=ALL), 'value'),
     Input(dict(type='input_gridColumn', index=ALL), 'value')],
    [State('signal_update_graphs', 'data'),
     State(dict(type='input_gridRow', index=ALL), 'id'),
     State(dict(type='input_gridColumn', index=ALL), 'id')]
)
def process_resize_input(input_grid_row, input_grid_column, signal_update_graphs,
                        input_grid_row_ids, input_grid_column_ids):

    """
    Process resize inputs than send signal_resize.
    signal_resize looks like:
    {'key': 'gridColumn' | 'gridRow',
     'index': [graph indices to change visuals for],
     'value': value to change to}
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('\n** process_resize_or_move_input**')
    logging.debug(callback_context)

    # only process if triggered by only one valid input or if all triggered are input_gridRow/Column (resize_all)
    if (len(callback_context) == 1 and callback_context != ['.']) \
            or all(['input_gridRow' in x for x in callback_context]) \
            or all(['input_gridColumn' in x for x in callback_context]):

        trigger_type = ''
        indices = []

        # determine the id indices of elements in callback context and the type
        # by extracting it from trigger string names
        for trigger in callback_context:
            temp = json.loads(trigger[trigger.index('{'):trigger.index('}') + 1])
            indices += [temp['index']]
            if not trigger_type:
                trigger_type = temp['type'][temp['type'].index('_') + 1:]

        if trigger_type == 'gridColumn':
            for i, item in enumerate(input_grid_column_ids):
                if item['index'] in indices:
                    if input_grid_column[i] is None:
                        raise PreventUpdate
                    return {'key': trigger_type, 'index': indices, 'value': input_grid_column[i]}

        elif trigger_type == 'gridRow':
            for i, item in enumerate(input_grid_row_ids):
                if item['index'] in indices:
                    if input_grid_row[i] is None:
                        raise PreventUpdate
                    return {'key': trigger_type, 'index': indices, 'value': input_grid_row[i]}

    raise PreventUpdate


@app.callback(
    Output('signal_delete', 'data'),
    [Input(dict(type='icon_delete', index=ALL), 'n_clicks'),
     Input('btn_close_all_active', 'n_clicks')],
    [State('signal_update_graphs', 'data'),
     State(dict(type='icon_delete', index=ALL), 'id'),
     State('store_active_graph', 'data')]
)
def process_delete_input(icon_delete, btn_close_all_active, signal_update_graphs, icon_delete_ids, store_active_graph):
    """
    Process input by delete icon or close all active button.
    Returns signal_delete list consisting of all graph indices to delete.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    if len(callback_context) == 1 and callback_context != ['.']:

        if callback_context == ['btn_close_all_active.n_clicks']:
            return store_active_graph  # delete all active graphs
        else:
            # determine which icon was clicked
            trigger = callback_context[0]
            trigger = json.loads(trigger[trigger.index('{'):trigger.index('}') + 1])
            trigger['type'] = trigger['type'][trigger['type'].index('_') + 1:]

            logging.debug(trigger['type'] + ' ' + str(trigger['index']))

            # verify that n_clicks of trigger is not None
            for i, item in enumerate(icon_delete_ids):
                if item['index'] == trigger['index']:
                    if icon_delete[i] is None:
                        raise PreventUpdate

            # return index if trigger in signal
            if trigger['type'] == 'delete':
                return [trigger['index']]

    raise PreventUpdate


########################################################################################
#                                                                                      #
#                                       ACTIVE GRAPH                                   #
#                                                                                      #
########################################################################################


@app.callback(
    Output(dict(type='checklist_active', index=ALL), 'value'),
    [Input(dict(type='icon_activate', index=ALL), 'n_clicks'),
     Input('btn_activate_all', 'n_clicks'),
     Input('signal_delete', 'data'),
     Input('signal_add_graph', 'data'),
     Input('signal_move', 'data')],
    [State(dict(type='checklist_active', index=ALL), 'id'),
     State('store_dashboard_config', 'data'),
     State('store_loaded_experiment', 'data'),
     State('store_active_graph', 'data')]
)
def set_checklist_active(nc1, nc2, signal_delete, signal_add_graph, signal_move, checklist_active_ids,
                         store_dashboard_config, code, store_active_graph):
    """
    Process the input of activate icon, activate all button, delete and add signal then
    adjusts graphs checklist_active accordingly.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('\n**set_checklist_active**')
    logging.debug(callback_context)

    if callback_context == ['.'] or len(callback_context) > 1:
        raise PreventUpdate

    # btn_activate_all -> activate all
    if callback_context == ['btn_activate_all.n_clicks']:
        checklist_active_values = [['active'] if str(item['index']) in store_dashboard_config[code] else []
                                   for item in checklist_active_ids]

    # signal_delete -> for deleted graphs remove active state, otherwise no_update
    elif callback_context == ['signal_delete.data']:
        checklist_active_values = [[] if item['index'] in signal_delete else no_update
                                   for item in checklist_active_ids]

    # signal_add_graph -> mark all graphs in active_graphs from signal_add_graph as active
    elif callback_context == ['signal_add_graph.data']:
        active_graphs = signal_add_graph.get('activate', [])
        checklist_active_values = [['active'] if item['index'] in active_graphs else []
                                   for item in checklist_active_ids]

    # signal_move -> keep active state for swapped graphs
    elif callback_context == ['signal_move.data']:
        # convert keys to int
        signal_move = {int(key): value for key, value in signal_move.items()}
        new_active_graphs = [signal_move[x] if x in signal_move else x for x in store_active_graph]
        if new_active_graphs == store_active_graph:
            raise PreventUpdate
        checklist_active_values = [['active'] if item['index'] in new_active_graphs else []
                                   for item in checklist_active_ids]

    # one icon_activate -> check the checklist for just this graph
    else:
        # determine trigger index
        trigger = callback_context[0]
        trigger = json.loads(trigger[trigger.index('{'):trigger.index('}') + 1])
        trigger['type'] = trigger['type'][trigger['type'].index('_') + 1:]

        # activate the trigger graph
        checklist_active_values = [['active'] if item['index'] == trigger['index'] else []
                                   for item in checklist_active_ids]

    return checklist_active_values


@app.callback(
    Output('store_active_graph', 'data'),
    [Input(dict(type='checklist_active', index=ALL), 'value'),
     Input('signal_add_graph', 'data')],
    [State(dict(type='checklist_active', index=ALL), 'id')]
)
def checklist_active_to_active_graph(checklist_active_values, signal_add_graph, checklist_active_ids):
    """
    Convert all checklist_active to an active_graph list stored in store_active_graph (session storage).
    active_graph contains the indices of active graphs.
    When triggered by signal_add_graph, the activate value in signal is used as the new active_graph list.
    """

    logging.debug('\n** checklist_active_to_active_graph **')
    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug(callback_context)

    # When triggered by signal_add_graph, use the activate value as active graphs.
    # The signal_add_graph input is necessary because the graphs to add do not exist yet
    # and so the checklist_actives do not exist yet either.
    if 'signal_add_graph.data' in callback_context:
        active_graph = signal_add_graph.get('activate', [])

    # otherwise translate checklist_active_ids to active graph list
    else:
        active_graph = []
        for i, item in enumerate(checklist_active_ids):
            if checklist_active_values[i] == ['active']:
                active_graph += [item['index']]

    return active_graph


########################################################################################
#                                                                                      #
#                                       HYBRID                                         #
#                                                                                      #
########################################################################################


@app.callback(
    [Output('graph_grid', 'className'),
     Output('filter_side', 'className'),
     Output('settings_side', 'className')],
    [Input('btn_toggle_filter', 'n_clicks'),
     Input('btn_toggle_settings', 'n_clicks')],
    [State('graph_grid', 'className'),
     State('filter_side', 'className'),
     State('settings_side', 'className')]
)
def toggle_side_menus(btn_toggle_filter, btn_toggle_settings, graph_grid_class_name, filter_side_class_name,
                      settings_side_class_name):
    """
    open/close filter/settings side menu when the corresponding button is pressed by
    adding/removing classes to the elements.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    # filter side menu
    if callback_context == ['btn_toggle_filter.n_clicks']:
        # close
        if (btn_toggle_filter % 2) == 0:
            filter_side_class_name = filter_side_class_name.replace('open', '')
            graph_grid_class_name = graph_grid_class_name.replace('m-l', '')
        # open
        else:
            filter_side_class_name += ' open'
            if graph_grid_class_name is None:
                graph_grid_class_name = ''
            graph_grid_class_name += ' m-l'

        return graph_grid_class_name, filter_side_class_name, no_update

    # settings side menu
    elif callback_context == ['btn_toggle_settings.n_clicks']:
        # close
        if (btn_toggle_settings % 2) == 0:
            settings_side_class_name = settings_side_class_name.replace('open', '')
            graph_grid_class_name = graph_grid_class_name.replace('m-r', '')
        # open
        else:
            settings_side_class_name += ' open'
            if graph_grid_class_name is None:
                graph_grid_class_name = ''
            graph_grid_class_name += ' m-r'

        return graph_grid_class_name, no_update, settings_side_class_name

    raise PreventUpdate


@app.callback(
    [Output('info_modal', 'style'),
     Output('info_modal_body', 'children'),
     Output('graph_grid', 'style')],
    [Input('btn_connection_info', 'n_clicks'),
     Input('btn_query_info', 'n_clicks'),
     Input('info_modal', 'n_clicks'),
     Input('span_close_modal', 'n_clicks'),
     Input('btn_experiment_info', 'n_clicks')],
    [State('store_loaded_experiment', 'data'),
     State('store_session_id', 'data'),
     State('table_query', 'active_cell'),
     State('table_dbms', 'active_cell')]
)
def toggle_info_modal(nc1, nc2, nc3, nc4, nc5, code, session_id, query_active_cell, connection_active_cell):
    """
    Open/close and adjust content of the query/connection/experiment information modal.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug(toggle_info_modal)
    logging.debug(callback_context)

    # close the modal and reset content
    if 'span_close_modal.n_clicks' in callback_context:
        return dict(display='none'), [], dict()

    else:
        content = []

        def get_modal_item(heading=None, pre=None, table=None, children=None, scale: float = 1):
            """
            Fill template modal element with heading, pre, table or other children.
            Use children input OR (heading and (pre OR table))
            """
            if children is None:
                if pre is not None:
                    child = html.Pre(style=dict(overflow='auto', maxHeight='100%'), children=pre)
                elif table is not None:
                    child = table
                else:
                    child = ''

                children = [
                    html.H5(heading),
                    html.Div(
                        style=dict(height='calc(100% - 70px)'),
                        children=child
                    )
                ]

            modal_item = html.Div(className='modal-item',
                                  style=dict(maxHeight=f'calc((100% - 60px) * {scale})'),
                                  children=children)
            return modal_item

        # connection
        if callback_context == ['btn_connection_info.n_clicks']:
            if connection_active_cell is None:
                raise PreventUpdate

            e = load_experiment(code)
            connection = connection_active_cell['row_id']
            content = [
                get_modal_item(heading='Connection Properties',
                               pre=json.dumps(e.get_experiment_connection_properties(connection), indent=2)),

                get_modal_item(heading='Initialization Script',
                               pre=e.get_experiment_initscript(connection))
            ]

        # query
        elif callback_context == ['btn_query_info.n_clicks']:

            if query_active_cell is None:
                raise PreventUpdate

            e = load_experiment(code)
            num_query = query_active_cell['row_id']

            # ************************************** Query Properties *************************************************

            query_properties = e.get_experiment_query_properties(num_query)
            query_properties['datastorage_size'] = tools.sizeof_fmt(e.get_datastorage_size(num_query))
            query_properties_json = json.dumps(query_properties, indent=2)

            # correct missing indention in query_properties_json
            groups = []
            for i, c in enumerate(query_properties_json):
                if c == ':':
                    colon = i
                if c == '\n':
                    pre = i
                    in_group = False
                if c == '\\':
                    if not in_group:
                        groups.append(dict(start=i, number=1, indent=colon - pre + 2))
                    else:
                        groups[-1]['number'] += 1
                    in_group = True

            for item in groups:
                query_properties_json = \
                    query_properties_json.replace('\\n', '\n' + ' ' * item['indent'], item['number'])

            # ************************************** Query Parameter **************************************************

            parameter_df = e.get_parameter_df(num_query).reset_index().rename(columns={'index': 'num run'})

            parameter_table = dash_table.DataTable(id='table_query_parameter',
                                                   style_data={
                                                       'whiteSpace': 'normal',
                                                       'height': 'auto'
                                                   },
                                                   style_cell={'textAlign': 'left'},
                                                   page_action='none',
                                                   style_table={
                                                       'maxHeight': '100%',
                                                       'overflowY': 'scroll'
                                                   },
                                                   columns=[{'name': i, 'id': i} for i in
                                                            parameter_df.columns],
                                                   data=parameter_df.to_dict('records')
                                                   ),

            # ************************************** Query Data Storage ***********************************************

            datastorage_df = pd.DataFrame()

            # gather data storage data
            for i in range(0, query_properties.get('numRun', -1)):
                try:
                    datastorage = e.get_datastorage_df(num_query, i)
                except IndexError:
                    continue
                if datastorage_df.empty:
                    datastorage_df = pd.DataFrame(columns=datastorage.columns)

                length = len(datastorage.index)
                for j in range(1, 1 + length):
                    if length == 1 and j == 1:
                        key = str(i + 1)  # start table at num run = 1
                    else:
                        key = f'{i + 1} ({j})'
                    datastorage_df.loc[key] = datastorage.loc[j]

            datastorage_df.index.name = 'num run'

            datastorage_table = dash_table.DataTable(
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto'
                },
                style_cell={'textAlign': 'left'},
                page_action='none',
                style_table={
                    'maxHeight': f'100%',
                    'overflowY': 'scroll'
                },
                columns=[{'name': i, 'id': i} for i in datastorage_df.reset_index().columns],
                data=datastorage_df.reset_index().to_dict('records'),
            )

            # ************************************** Query Received Sizes *********************************************

            received_sizes = e.get_received_sizes(num_query)
            received_sizes_pretty = {key: tools.sizeof_fmt(value) for key, value in received_sizes.items()}
            received_sizes_df = pd.DataFrame.from_dict(data=received_sizes_pretty,
                                                       orient='index',
                                                       columns=['received_size'])

            received_sizes_df.index.name = 'Connection'

            received_sizes_table = dash_table.DataTable(
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto'
                },
                style_cell={'textAlign': 'left'},
                page_action='none',
                style_table={
                    'maxHeight': f'100%',
                    'overflowY': 'scroll'
                },
                columns=[{'name': i.capitalize(), 'id': i} for i in
                         received_sizes_df.reset_index().columns],
                data=received_sizes_df.reset_index().to_dict('records'),
            )

            # ************************************** Resultset Dict *********************************************

            resultset_dict = e.get_resultset_dict(num_query)
            ind = pd.MultiIndex.from_arrays([[]] * 2, names=('num_run', 'resultset_row'))
            col = pd.MultiIndex.from_arrays([[]] * 2, names=('connection', 'result_heading'))
            resultset_df = pd.DataFrame(columns=col, index=ind)

            # fill MultiIndex resultset_df with data from resultset_dict
            for connection, resultset in resultset_dict.items():
                if resultset:
                    cols = resultset[0][0]

                    for num_run, num_run_data in enumerate(resultset, 1):
                        for num_run_num, num_run_num_data in enumerate(num_run_data[1:], 1):
                            for i, c in enumerate(cols):
                                resultset_df.loc[(num_run, num_run_num), (connection, c)] = num_run_num_data[i]

            resultset_dict_table_columns = [{"name": ["", "num run"], "id": "num_run"}] + \
                                           [{"name": list(x), "id": f'{x[0]}_{x[1]}'} for x in resultset_df.keys()]

            resultset_dict_table_data = [{'num_run': f'{key[0]} ({key[1]})',
                                          **{f'{k[0]}_{k[1]}': v for k, v in value.items()}}
                                         for key, value in resultset_df.T.to_dict().items()]

            resultset_dict_table = dash_table.DataTable(
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto'
                },
                style_cell={'textAlign': 'left'},
                page_action='none',
                style_table={
                    'maxHeight': f'100%',
                    'overflowY': 'scroll'
                },
                columns=resultset_dict_table_columns,
                data=resultset_dict_table_data,
                merge_duplicate_headers=True,
            )

            # ************************************** Combine Content ***********************************************
            content = [
                get_modal_item(heading='Query Properties', pre=query_properties_json, scale=1 / 2),
                get_modal_item(heading='Parameter', table=parameter_table, scale=1 / 2),
                get_modal_item(heading='datastorage_table', table=datastorage_table, scale=1 / 2),
                get_modal_item(heading='received_sizes_table', table=received_sizes_table, scale=1 / 2),
                get_modal_item(heading='resultset_dict_table', table=resultset_dict_table, scale=1 / 2),

            ]

        # experiment
        elif callback_context == ['btn_experiment_info.n_clicks']:
            if code is None:
                raise PreventUpdate

            e = load_experiment(code)

            workload_properties = copy.deepcopy(e.get_experiment_workload_properties())
            workload_properties['numQueries'] = preview.loc[code]['queries']
            workload_properties['numConnections'] = preview.loc[code]['connections']

            results = workload_properties.pop('results', None)

            experiment_workflow = e.get_experiment_workflow().reset_index()

            workflow_table = dash_table.DataTable(id='table_experiment_workflow',
                                                  style_data={
                                                      'whiteSpace': 'normal',
                                                      'height': 'auto'
                                                  },
                                                  style_cell={'textAlign': 'left'},
                                                  page_action='none',
                                                  style_table={
                                                      'maxHeight': '100%',
                                                      'overflow': 'scroll'
                                                  },
                                                  columns=[{'name': i.capitalize(), 'id': i} for i in
                                                           experiment_workflow.columns],
                                                  data=experiment_workflow.to_dict('records')
                                                  ),

            content = [
                get_modal_item(heading='Experiment Workload Properties',
                               pre=json.dumps(workload_properties, indent=2)),

                get_modal_item(heading='Experiment Workflow', table=workflow_table),

                get_modal_item(heading='Results', pre=json.dumps(results, indent=2)),
            ]

        if content:
            return dict(display='block'), content, dict(marginLeft='50%')

    raise PreventUpdate


########################################################################################
#                                                                                      #
#                                  MANAGE CONFIG STORE                                 #
#                                                                                      #
########################################################################################


@app.callback(
    Output('signal_add_graph', 'data'),
    [Input('store_loaded_experiment', 'data'),
     Input('btn_add_empty', 'n_clicks'),
     Input('btn_duplicate_graph', 'n_clicks'),
     Input('btn_add_preset', 'n_clicks'),
     Input('btn_add_table', 'n_clicks'),
     Input('btn_add_investigate_q1', 'n_clicks'),
     Input('signal_load_favorite', 'data')],
    [State('store_active_graph', 'data'),
     State('store_dashboard_config', 'data'),
     State('load_predefined_dashboard', 'on')]
)
def process_add_graph(code, btn_add_empty, btn_duplicate_graph, btn_add_preset, btn_add_table,
                      btn_add_investigate_q1, signal_load_favorite, active_graph,
                      store_dashboard_config, load_predefined_dashboard):
    """
    Process all inputs that add graphs.
    Results in a signal_add_graph which includes the graphs to add, which graphs to activate and an optional
    reset attribute to reset the grid before creation.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('\n** process_add_graph **')
    logging.debug(callback_context)
    logging.debug(active_graph)

    connection_indices = list(range(preview.loc[code]['connections']))

    # ************************************ Experiment changed *******************************************************
    if 'store_loaded_experiment.data' in callback_context:
        # load graphs from store_dashboard_config
        add = {int(key): value for key, value in store_dashboard_config.get(code, dict()).items()}

        if load_predefined_dashboard:
            # if experiment is new to dashboard config, or there is no graph or there is only the default graph
            # in config, then allow loading the predefined dashboard.

            if add == {0: Graph(connection_indices=connection_indices).__dict__} \
                    or code not in store_dashboard_config \
                    or len(store_dashboard_config.get(code)) == 0:

                # add predefined graphs to config
                add = predefined_dashboard(connection_indices)

        elif code not in store_dashboard_config:  # create a default graph because code is new to config
            # add default graph to config
            add = {0: Graph(connection_indices=connection_indices).__dict__}

        if len(add) == 0:
            raise PreventUpdate

        return dict(add=add, activate=[max(add.keys())], reset=True)

    # ************************************ Add Preset or empty Graph *************************************************
    elif callback_context[0] in ['btn_add_preset.n_clicks', 'btn_add_empty.n_clicks']:
        new_index = get_new_indices(store_dashboard_config[code])[0]

        if callback_context == ['btn_add_preset.n_clicks']:
            new_graph = Graph(graph_type='Preset',
                              preset='heatmap_errors',
                              connection_indices=connection_indices).__dict__

        elif callback_context == ['btn_add_empty.n_clicks']:
            new_graph = Graph(connection_indices=connection_indices).__dict__

        return dict(add={new_index: new_graph}, activate=[new_index])

    # ************************************ Add Duplicate or add Table ***********************************************
    elif callback_context[0] in ['btn_duplicate_graph.n_clicks', 'btn_add_table.n_clicks']:
        # add statistics table or duplicate for every active graph
        new_graphs = dict()

        new_indices = get_new_indices(store_dashboard_config[code], number=len(active_graph))

        for i, index in enumerate(active_graph):
            new_graphs[new_indices[i]] = dict(store_dashboard_config[code][str(index)])
            if callback_context == ['btn_add_table.n_clicks']:
                new_graphs[new_indices[i]]['graph_type'] = 'Table Statistics'

        return dict(add=new_graphs, activate=[max(new_indices)])

    # ************************************ Add Investigate Q1 ********************************************************
    elif callback_context == ['btn_add_investigate_q1.n_clicks']:
        investigate_graphs = investigate_q1(connection_indices)
        new_indices = get_new_indices(store_dashboard_config[code], number=len(investigate_graphs))
        new_graphs = {new_indices[i]: investigate_graphs[i].__dict__ for i in range(0, len(investigate_graphs))}

        return dict(add=new_graphs, activate=list(new_graphs.keys()))

    # ************************************ Load Favorites **********************************************************
    elif callback_context == ['signal_load_favorite.data']:
        # Load and RESET
        if signal_load_favorite.get('reset'):

            return dict(add=signal_load_favorite.get('config'),
                        activate=signal_load_favorite.get('active_graphs', []),
                        reset=True)

        # Load and APPEND
        else:
            config = signal_load_favorite.get('config')
            config_indices = list(map(int, config.keys()))
            config_indices.sort()

            activate_old = signal_load_favorite.get('active_graphs', [])
            new_indices = get_new_indices(store_dashboard_config[code], number=len(config))
            new_graphs = {new_indices[i]: config[str(config_indices[i])] for i in range(0, len(config))}
            activate_new = [new_indices[i] for i in range(0, len(config)) if config_indices[i] in activate_old]

            return dict(add=new_graphs, activate=activate_new)

    raise PreventUpdate


@app.callback(
    [Output('store_dashboard_config', 'data'),
     Output('signal_update_graphs', 'data')],
    [Input('signal_resize', 'data'),
     Input('signal_move', 'data'),
     Input('signal_update_settings', 'data'),
     Input('signal_delete', 'data'),
     Input('signal_add_graph', 'data')],
    [State('store_active_graph', 'data'),
     State('store_dashboard_config', 'data'),
     State('store_loaded_experiment', 'data')]
)
def update_store(signal_resize, signal_move, signal_update_settings, signal_delete, signal_add_graph,
                 active_graph, store_dashboard_config, code):
    """
    Process all signals that lead to a change in stored dashboard config.
    Results in an updated dashboard config and a signal_update_graphs for create_widgets callback.
    """

    logging.debug('\n** update_store **')
    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug(callback_context)
    logging.debug(active_graph)

    # ************************************ Multiple Triggers ********************************************************
    if len(callback_context) > 1 or callback_context == ['.']:
        raise PreventUpdate

    # ************************************ Signal Add Graph ***********************************************************
    elif callback_context == ['signal_add_graph.data']:
        if code not in store_dashboard_config:
            store_dashboard_config[code] = dict()

        logging.debug(signal_add_graph)
        add = signal_add_graph.get('add', dict())
        # add new graphs to config
        for key, value in add.items():
            store_dashboard_config[code][str(key)] = value

        # adjust signal to create new graphs
        signal_update_graphs = dict(create=list(map(int, add.keys())))

        if signal_add_graph.get('reset', False):
            signal_update_graphs['reset'] = True

        return store_dashboard_config, signal_update_graphs

    # ************************************ Signal Resize ***********************************
    elif callback_context == ['signal_resize.data']:
        # save new size in config and don't request graph update
        for index in signal_resize['index']:
            store_dashboard_config[code][str(index)][signal_resize['key']] = signal_resize['value']
        return store_dashboard_config, no_update

    # ************************************ Signal Move ***********************************
    elif callback_context == ['signal_move.data']:
        keys = list(signal_move.keys())
        i1 = str(signal_move[keys[0]])
        i2 = str(signal_move[keys[1]])

        # swap configurations
        store_dashboard_config[code][i1], store_dashboard_config[code][i2] = \
            store_dashboard_config[code][i2], store_dashboard_config[code][i1]

        # return updated store_dashboard_config and request rerender of swapped graphs
        return store_dashboard_config, dict(swap=signal_move)

    # ************************************ Signal Update Settings ***************************************************
    elif callback_context == ['signal_update_settings.data']:
        # save which graphs to update
        update_list = set()

        for index in active_graph:
            for key, value in signal_update_settings.items():

                # don't allow update of certain attributes for preset graphs
                if store_dashboard_config[code][str(index)]['graph_type'] == 'Preset':
                    if key in ['type', 'name']:
                        # allow type update for barchart run drilldown only
                        if not(key == 'type' and
                               store_dashboard_config[code][str(index)]['preset'] == 'barchart_run_drill'):
                            continue

                # update config
                store_dashboard_config[code][str(index)][key] = value

                update_list.add(index)

        # return updated config and request graph update for changed graphs
        return store_dashboard_config, dict(update=list(update_list))

    # ************************************ Signal Delete ************************************************************
    elif ['signal_delete.data'] == callback_context:
        # delete all configs with index in signal_delete
        for index in signal_delete:
            del store_dashboard_config[code][str(index)]

        # return updated config and request deletion of widgets
        return store_dashboard_config, dict(delete=signal_delete)


########################################################################################
#                                                                                      #
#                                       GRAPH                                          #
#                                                                                      #
########################################################################################


@app.callback(
    [Output('graph_grid', 'children'),
     Output(dict(type='update', index=ALL), 'children')],
    [Input('signal_update_graphs', 'data')],
    [State('store_dashboard_config', 'data'),
     State('graph_grid', 'children'),
     State('store_loaded_experiment', 'data'),
     State(dict(type='update', index=ALL), 'id'),
     State('store_active_graph', 'data')]
)
def create_widgets(signal_update_graphs, store_dashboard_config, graph_grid, code, update_ids, active_graph):
    """
    Process input of signal_update_graphs.
    Possible actions are update, swap, delete or create graphs.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    logging.debug('\n** create_widgets **')
    logging.debug(callback_context)
    logging.debug(signal_update_graphs)

    dashboard_config = store_dashboard_config.get(code)

    if 'update' in signal_update_graphs:
        # do not update grid and call update for every graph in signal
        return no_update, [1 if x['index'] in signal_update_graphs['update'] else no_update for x in update_ids]

    if 'swap' in signal_update_graphs:
        # convert keys to int
        swap = {int(key): value for key, value in signal_update_graphs.get('swap').items()}

        grid_indices = []

        # translate graph index into grid index
        for i in range(0, len(graph_grid)):
            if graph_grid[i]['props']['id']['index'] in swap:
                grid_indices += [i]
                if len(grid_indices) == 2:
                    break

        # swap graph container children
        graph_grid[grid_indices[0]]["props"]['children'][0]["props"]['children'][0]['props']['children'],\
            graph_grid[grid_indices[1]]["props"]['children'][0]["props"]['children'][0]['props']['children'] = \
            graph_grid[grid_indices[1]]["props"]['children'][0]["props"]['children'][0]['props']['children'], \
            graph_grid[grid_indices[0]]["props"]['children'][0]["props"]['children'][0]['props']['children']

        # adjust graph index too
        for i in grid_indices:
            children = graph_grid[i]["props"]['children'][0]["props"]['children'][0]['props']['children']

            if not children:
                continue

            elif children['type'] == 'Graph':
                children['props']['id']['index'] = swap[children['props']['id']['index']]

            elif children['type'] == 'Div':  # table
                children['props']['children'][1]['props']['id']['index'] = \
                    swap[children['props']['children'][1]['props']['id']['index']]

            graph_grid[i]["props"]['children'][0]["props"]['children'][0]['props']['children'] = children

        return graph_grid, [no_update] * len(update_ids)

    if 'delete' in signal_update_graphs:
        # remove the deleted graphs in signal from grid
        graph_grid = [x for x in graph_grid if x['props']['id']['index'] not in signal_update_graphs['delete']]

        # return altered grid and don't update graphs
        return graph_grid, [no_update] * len(update_ids)

    if 'create' in signal_update_graphs:

        # reset grid if desired
        if signal_update_graphs.get('reset'):
            graph_grid = []

        create = signal_update_graphs['create']

        # create requested graphs
        for index in create:

            grid_column = dashboard_config[str(index)]["gridColumn"]
            grid_row = dashboard_config[str(index)]["gridRow"]
            style = dict(gridColumn=f'span {grid_column}', gridRow=f'span {grid_row}')

            widget = html.Div(
                id=dict(type='div', index=index),
                className='active-graph' if index in active_graph else '',
                style=style,
                children=[

                    dcc.Loading(
                        children=[
                            # graph container
                            html.Div(
                                id=dict(type='graph_container', index=index),
                                style=dict(height=f'calc(((100vh - 80px)/3 * {grid_row} - (3-{grid_row}) * 10px))'),
                                className='graph_container',
                                children=[]
                            )
                        ]
                    ),

                    # update graph signal
                    html.Div(id=dict(type='update', index=index), style=dict(display='none')),

                    # grid columns
                    dcc.Input(
                        id=dict(type='input_gridColumn', index=index), type="number",
                        className='input_size input_gridColumn', min=3, max=12, step=3,
                        value=grid_column
                    ),

                    # grid rows
                    dcc.Input(
                        id=dict(type='input_gridRow', index=index), type="number", className='input_size input_gridRow',
                        min=1, max=3, step=1,
                        value=grid_row
                    ),

                    # move up button
                    html.Div(id=dict(type='icon_move_up', index=index),
                             className='icon_move_up icon'),

                    # move down button
                    html.Div(id=dict(type='icon_move_down', index=index),
                             className='icon_move_down icon'),

                    # active checklist
                    dcc.Checklist(
                        id=dict(type='checklist_active', index=index),
                        className='input_size checklist_active',
                        options=[
                            {'label': 'Active', 'value': 'active'},
                        ],
                        value=['active'] if index in active_graph else []
                    ),

                    # download button
                    html.A(id=dict(type='icon_download', index=index),
                           className='icon_download icon',
                           download="data.csv",
                           href="",
                           target="_blank"),

                    # activate button
                    html.Div(id=dict(type='icon_activate', index=index),
                             className='icon_activate icon'),

                    # delete button
                    html.Div(id=dict(type='icon_delete', index=index),
                             className='icon_delete icon')
                ]
            )

            graph_grid.insert(0, widget)

        # return grid and request update for new created graphs only
        return graph_grid, [1 if x['index'] in create else no_update for x in update_ids]


@app.callback(
    [Output(dict(type='graph_container', index=MATCH), 'children'),
     Output(dict(type='icon_download', index=MATCH), 'href'),
     Output(dict(type='icon_download', index=MATCH), 'download')],
    [Input(dict(type='update', index=MATCH), 'children')],
    [State('store_dashboard_config', 'data'),
     State('store_loaded_experiment', 'data'),
     State('store_session_id', 'data'),
     State(dict(type='update', index=MATCH), 'id')]
)
def update_figure(update_figure, store_dashboard_config, code, session_id, id):
    """
    This is called whenever graphs "update" elements children is changed (during creation too).
    It causes a render of graphs content.
    Also links the DataFrame csv and title to graphs download icon.
    """

    index = id['index']
    dashboard_config = store_dashboard_config[code][str(index)]

    logging.debug('\n** update_figure **')
    logging.debug(index)

    graph, df = Graph(**dashboard_config).render(code, session_id, index)

    df_csv = df.to_csv(index=True, encoding='utf-8', sep=';', decimal='.')
    csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(df_csv)

    title = df.name
    title = code + '_' + title
    title = re.sub(' ', '', title)
    title = re.sub(':', '', title)
    title = re.sub(r'\|', '_', title)
    title += '.csv'

    return graph, csv_string, title


@app.callback(
    [Output(dict(type='div', index=ALL), 'style'),
     Output(dict(type='graph_container', index=ALL), 'style')],
    [Input('signal_resize', 'data')],
    [State(dict(type='div', index=ALL), 'id'),
     State(dict(type='div', index=ALL), 'style'),
     State(dict(type='graph_container', index=ALL), 'style')]
)
def change_graph_shape(signal_resize, ids, div_style, graph_style):
    """
    React on signal_resize and update graphs shape accordingly.
    Both, the divs space in grid and the graphs height are adjusted.
    This is necessary because Plotly graphs won't have correct height otherwise.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]
    logging.debug('** change_graph_shape **')
    logging.debug(callback_context)
    logging.debug(signal_resize)

    if signal_resize is None:
        raise PreventUpdate

    for i, item in enumerate(ids):
        if item['index'] in signal_resize['index']:
            if signal_resize['key'] == 'gridColumn':
                div_style[i]['gridColumn'] = f'span {signal_resize["value"]}'
                graph_style[i] = no_update
            elif signal_resize['key'] == 'gridRow':
                grid_row = signal_resize['value']
                div_style[i]['gridRow'] = f'span {grid_row}'
                graph_style[i] = dict(height=f'calc(((100vh - 80px)/3 * {grid_row} - (3-{grid_row}) * 10px))')

        else:
            div_style[i] = no_update
            graph_style[i] = no_update

    return div_style, graph_style


@app.callback(
    Output(dict(type='div', index=ALL), 'className'),
    [Input('store_active_graph', 'data')],
    [State(dict(type='div', index=ALL), 'id'),
     State(dict(type='div', index=ALL), 'className')]
)
def highlight_active_graphs(active_graph, ids, class_name):
    """
    Highlight all graphs in store_active_graph.
    """

    if len(ids) == 0:
        raise PreventUpdate

    else:
        for i, element in enumerate(ids):
            if element.get('index') in active_graph:
                if 'active-graph' not in class_name[i]:
                    class_name[i] += ' active-graph'
            else:
                if 'active-graph' in class_name[i]:
                    class_name[i] = class_name[i].replace('active-graph', '')

        return class_name


########################################################################################
#                                                                                      #
#                                       FAVORITES                                      #
#                                                                                      #
########################################################################################


@app.callback(
    [Output('table_favorites', 'data'),
     Output('favorites_table', 'style')],
    [Input('store_loaded_experiment', 'data'),
     Input('store_favorites', 'data')]
)
def render_favorites_table(code, favorites):
    """
    Update when favorites or experiment change(s).
    When table is empty -> hide table.
    """

    if favorites is None or code is None:
        return no_update, {'display': 'none'}

    if code not in favorites:
        return no_update, {'display': 'none'}

    if len(favorites[code]) == 0:
        return no_update, {'display': 'none'}

    data = []
    tooltip_data = []
    for key, value in favorites[code].items():

        try:
            t_create = time.strftime("%H:%M:%S, %d.%m.%Y", tuple(value.get('t_create')))
        except:
            t_create = 'None'

        d = dict(fav_id=key,
                 id=key,
                 code=code,
                 t_create=t_create,
                 comment=value.get('comment'),
                 number_graphs=len(value.get('config').keys()),
                 )
        data.append(d)

        # tooltip = {'config': {'type': 'text', 'value': pretty_markdown(value.get('config'), '', indent=2),
        #                       'delay': 50, 'duration': 36000}}
        #tooltip_data.append(tooltip)

    return data, {'display': 'block'}


@app.callback(
    Output('signal_load_favorite', 'data'),
    [Input('btn_load_favorite', 'n_clicks'),
     Input('btn_load_append_favorite', 'n_clicks')],
    [State('table_favorites', 'selected_rows'),
     State('table_favorites', 'data'),
     State('store_favorites', 'data'),
     State('store_loaded_experiment', 'data')]
)
def load_favorite(nc1, nc2, selected_rows, table_data, favorites, code):
    """
    Signal load or load append user input.
    signal_load_favorite contains the favorite element + reset attribute
    """

    if nc1 is None and nc2 is None:
        raise PreventUpdate

    if selected_rows is None or selected_rows == []:
        raise PreventUpdate

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    fav_id = table_data[selected_rows[0]]['fav_id']

    signal = favorites.get(code).get(fav_id)

    if callback_context == ['btn_load_favorite.n_clicks']:
        signal['reset'] = True
    elif callback_context == ['btn_load_append_favorite.n_clicks']:
        signal['reset'] = False

    logging.debug(signal)
    return signal


@app.callback(
    [Output('store_favorites', 'data'),
     Output('download_favorites', 'download'),
     Output('download_favorites', 'href'),
     Output('upload_alert', 'displayed')],
    [Input('btn_add_favorite', 'n_clicks'),
     Input('btn_remove_favorite', 'n_clicks'),
     Input('upload_favorites', 'contents')],
    [State('store_loaded_experiment', 'data'),
     State('table_favorites', 'selected_rows'),
     State('table_favorites', 'data'),
     State('store_favorites', 'data'),
     State('store_active_graph', 'data'),
     State('store_dashboard_config', 'data'),
     State('input_favorite_comment', 'value'),
     State('upload_favorites', 'filename')]
)
def change_favorites(add_n_clicks, remove_n_clicks, upload_contents, code, selected_rows, table_data,
                     favorites, active_graph, dashboard_config, comment, upload_filename):
    """
    Remove or add favorite depending on user input.
    """

    callback_context = [p['prop_id'] for p in dash.callback_context.triggered]

    # remove
    if ['btn_remove_favorite.n_clicks'] == callback_context:
        if selected_rows is None or selected_rows == []:
            raise PreventUpdate

        fav_id = table_data[selected_rows[0]]['fav_id']
        favorites[code].pop(fav_id, None)

    # add
    elif ['btn_add_favorite.n_clicks'] == callback_context:
        if favorites is None:
            fav_key = 0
            favorites = {code: {}}
        elif code not in favorites:
            fav_key = 0
            favorites[code] = {}
        else:
            fav_key = 0 if len(favorites[code].keys()) == 0 else max(map(int, favorites[code].keys())) + 1

        fav_data = dict(config=dashboard_config[code],
                        comment=comment,
                        t_create=time.localtime(),
                        active_graphs=active_graph)

        favorites[code][fav_key] = fav_data

        # with open(code+'.json') as fr:
        #     try:
        #         data = json.load(fr)
        #     except json.decoder.JSONDecodeError:
        #         data = dict()
        #
        #     with open(code+'.json', 'w', encoding='utf-8') as fw:
        #         json.dump(data, fw, ensure_ascii=False, indent=2)

    elif callback_context == ['upload_favorites.contents']:

        if '.json' not in upload_filename:
            return no_update, no_update, no_update, True

        if upload_contents is not None:
            content_type, content_string = upload_contents.split(',')
            decoded = base64.b64decode(content_string)
            new_favorites = json.loads(decoded)
            experiment = new_favorites.get('experiment')
            new_favorites.pop('experiment', None)

            if experiment != code:
                return no_update, no_update, no_update, True

            favorites[experiment] = new_favorites

    else:
        raise PreventUpdate

    favorites_json = dict(favorites[code])
    favorites_json['experiment'] = code
    favorites_json = "data:text/json;charset=utf-8," + urllib.parse.quote(json.dumps(favorites_json, indent=2))

    return favorites, f'favorites_{code}.json', favorites_json, False


########################################################################################
#                                                                                      #
#                                                                                      #
#                                    END CALLBACKS                                     #
#                                                                                      #
#                                                                                      #
########################################################################################

def startup():
    global app, preview, evaluate
    #logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.ERROR)



    parser = argparse.ArgumentParser(description='Dashboard for interactive inspection of dbmsbenchmarker results.')
    parser.add_argument('-r', '--result-folder',
                        help='Folder storing benchmark result files.',
                        default='./')  # set your own default path here
    parser.add_argument('-a', '--anonymize',
                        help='Anonymize all dbms.',
                        action='store_true',
                        default=False)
    parser.add_argument('-u', '--user',
                        help='User name for auth protected access.',
                        default=None)
    parser.add_argument('-p', '--password',
                        help='Password for auth protected access.',
                        default=None)
    parser.add_argument('-d', '--debug',
                        help='Show debug information.',
                        action='store_true',
                        default=False)

    args = parser.parse_args()
    result_path = args.result_folder
    # verify that result path was given
    if result_path is None:
        raise ValueError('No result path was given.')

    # create inspector instance using the result path
    evaluate = inspector.inspector(result_path, anonymize=args.anonymize)
    # preview of all available experiments in result path
    preview = evaluate.get_experiments_preview()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)


    # Create Dash App Auth instance
    if args.user is not None and args.password is not None:
        VALID_USERNAME_PASSWORD_PAIRS = { # Keep this out of source code repository - save in a file or a database
            args.user: args.password
        }
        auth = dash_auth.BasicAuth(
            app,
            VALID_USERNAME_PASSWORD_PAIRS
        )

    # Assign layout to app
    app.layout = layout.serve_layout(preview)
    app.run_server(debug=args.debug, host='0.0.0.0', threaded=True, processes=1)
