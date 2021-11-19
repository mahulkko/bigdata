#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Import
########################################
from pyspark.sql import SQLContext, SparkSession 
from pyspark.sql.types import StructType, StringType, TimestampType

import dash
from dash import html
from dash import dcc

import plotly.express as px
import plotly.graph_objects as go



# Creat Spark Session
########################################
spark = SparkSession.builder.appName('Spritpreise').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(spark.sparkContext)


# Map Access Token
########################################
mapbox_access_token = open(".mapbox_token").read()



# Imoport Gas Stations
##########################################
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

# station_schema.show()
# station_schema.printSchema();



# Import Gas Price from a day
################################################
print("2. Laden von Spritpreisen.csv")
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

# Colect all Stations
########################
station_schema_collect = station_schema.collect()
station_count = len(station_schema_collect)
#station_count = 1000



# Creat Arrays for Latitude, Longitute and Name
##################################################
lat_list = []
lon_list = []
name_list = []

for i in range(1, station_count-1):
    lon_list.append(float(station_schema_collect[i][8]))
    lat_list.append(float(station_schema_collect[i][7]))
    name_list.append(station_schema_collect[i][1])
    
    

# Create MAP
#################################################
fig = go.Figure(go.Scattermapbox(
        lat=lat_list,
        lon=lon_list,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=8,
            color='rgb(242, 177, 172)',
        ),
        text=name_list,
    ))

# Update MAP Layout
#################################################
fig.update_layout(
   autosize=True,
   margin_l=0,
   margin_r=0,
   margin_t=0,
   margin_b=0,
   hovermode='closest',
   mapbox=dict(
       accesstoken=mapbox_access_token,
       bearing=0,
       center=go.layout.mapbox.Center(
           lat=51,
           lon=10
       ),
       pitch=0,
       zoom=5
   )
)


test_piv = gas_schema.filter(gas_schema.uuid == station_schema.collect()[1][0])

# Define the Layout
################################################
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
                        
                           dcc.Graph(id='gas_price',
                                                  config={'displayModeBar': False},
                                                  animate=True,
                                                  figure=px.line(test_piv.toPandas(),
                                                                 x='date',
                                                                 y=['diesel', 'e5', 'e10'],
                                                                 template='plotly_dark').update_layout(
                                                      {'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                                                       'paper_bgcolor': 'rgba(0, 0, 0, 0)'},
                                                      yaxis_title='Preis',
                                                      xaxis_title='Uhrzeit',
                                                      legend_title='Benzinart',
                                                      title=station_schema.collect()[1][1])
                                                  )
                        
                       ],
                    style={'color': '#1E1E1E'})
                                     
                    ]
                ),  
                
            # Define the right element
            html.Div(className='eight columns div-for-charts bg-grey',
                children = [
                    
                    # Show Map
                    ###################################                      
                    dcc.Graph(id='gas_stations', 
                              style={'width': '100vh', 'height': '100vh'}, 
                              figure=fig
                )
                ])                       
            ])     
    ])


# Run the App
################################################

if __name__ == "__main__":
    app.run_server(debug=True)






















