#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dash
import pandas as pd
from dash import html

from dash import dcc
import plotly.express as px
import plotly.graph_objects as go

from pyspark.sql import SQLContext, SparkSession 
from pyspark.sql.types import StructType, DateType, StringType, IntegerType, TimestampType



spark = SparkSession.builder.appName('Spritpreise').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(spark.sparkContext)

#
# Lade Tankstellen Stationen als Schema
#
print("1. Laden von Tankstellen Stationen.csv")
station_schema = StructType()
station_schema.add("uuid",StringType(),True)
station_schema.add("name",StringType(),True)
station_schema.add("brand",StringType(),True)
station_schema.add("street",StringType(),True)
station_schema.add("house_number",StringType(),True)
station_schema.add("post_code",StringType(),True)
station_schema.add("city",StringType(),True)
station_schema.add("latitude",StringType(),True)
station_schema.add("logitude",StringType(),True)
station_schema.add("first_active",StringType(),True)
station_schema.add("opening_times_json",StringType(),True)

station_schema = sqlContext.read.options(delimiter=',').schema(station_schema).csv('2021-11-05-stations.csv')

station_schema.show()
station_schema.printSchema();


#
# Lade Benzinpreise von einem Tag als Schema
#
print("2. Laden von Spritpresien.csv")
gas_schema = StructType()
gas_schema.add("date",TimestampType(),True)
gas_schema.add("uuid",StringType(),True)
gas_schema.add("diesel",StringType(),True)
gas_schema.add("e5",StringType(),True)
gas_schema.add("e10",StringType(),True)
gas_schema.add("diesel_change",StringType(),True)
gas_schema.add("e5_change",StringType(),True)
gas_schema.add("e10_change",StringType(),True)

gas_schema = sqlContext.read.options(delimiter=',').schema(gas_schema).csv('2021-11-05-prices.csv')

gas_schema.show()
gas_schema.printSchema();


print("Änderung Spritpreise")

# [row][colum]
test_piv = gas_schema.filter(gas_schema.uuid == station_schema.collect()[1][0])
test_piv.show()

#gas_station = station_schema.join(test_piv, station_schema.uuid == test_piv.uuid,"inner")
#gas_station.show()

dict_list2 = []
station_schema_collect = station_schema.collect()
#station_schema.count()-2
for i in range(1, 100):
        dict_list2.append({'label': station_schema_collect[i][1], 'value': station_schema_collect[i][0]})

lat_list = []
lon_list = []
name_list = []
for i in range(1, 100):
        lon_list.append({'lon': float(station_schema_collect[i][8])})
        lat_list.append({'lat': float(station_schema_collect[i][7])})
        name_list.append({'name': station_schema_collect[i][1]})

fig = go.Figure(data=go.Scattergeo(
    lon = lat_list,
    lat = lon_list,
    text = name_list,
    mode = 'markers'
))

fig.update_layout(
    geo_scope='europe'
)


app = dash.Dash(__name__)
app.layout = html.Div(
    children = [html.Div(className='row',  # Define the row element
        children = [
        
            # Define the left element
            html.Div(className=  'four columns div-user-controls',
                children = [
                    html.H2('DASH - STOCK PRICES'),
                    html.P('Visualising time series with Plotly - Dash.'),
                    html.P('Pick one or more stocks from the dropdown below.'),
                    
                    html.Div(
                       className='div-for-dropdown',
                       children=[
                        dcc.Dropdown(id='stockselector', options=dict_list2,
                            multi=False, 
                            style={'backgroundColor': '#1E1E1E'},
                            className='stockselector'
                           ),
                       ],
                    style={'color': '#1E1E1E'})
                                     
                    ]
                ),  
                
            # Define the right element
            html.Div(className='eight columns div-for-charts bg-grey',
                children = [
                

                dcc.Graph(id='timeseries',
                config={'displayModeBar': False},
                animate=True,
                    figure=px.line(test_piv.toPandas(),
                         x='date',
                         y=['diesel','e5','e10'],
                         template='plotly_dark').update_layout(
                                   {'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                                    'paper_bgcolor': 'rgba(0, 0, 0, 0)'},
                                    yaxis_title='Preis',
                                    xaxis_title='Uhrzeit',
                                    legend_title='Benzinart',
                                    title=station_schema.collect()[1][1])
                )

                
                ])                       
            ])     
    ])


if __name__ == "__main__":
    app.run_server(debug=True)
