#!/usr/bin/env python

# coding: utf-8

# In[38]:

import json
import requests
import pymongo
from xml.dom import minidom


# Création de la connexion à la base mongo, de la base mydb et de 4 collections (park, air, sub, pool)

# In[52]:

client = pymongo.MongoClient()
db = client.mydb
park = db.park
air = db.air
sub = db.sub
pool = db.pool


# Téléchargement des 4 bases de données

# In[204]:

dataAirQuality = requests.get("https://data.cityofnewyork.us/resource/asyy-77dj.json").json()
datSubway = requests.get("https://data.cityofnewyork.us/resource/kk4q-3rt2.json").json()
dataPark = requests.get("https://data.cityofnewyork.us/api/geospatial/g84h-jbjm?method=export&format=GeoJSON").json()
dataPool = requests.get("https://www.nycgovparks.org/bigapps/DPR_Pools_indoor_001.xml")


# Insertion des données sur la qualité de l air

# In[208]:

res = air.insert_many(dataAirQuality)
len(res.inserted_ids)


# Insertion des parcs. On garde que les champs : name (s'il existe), area, latitude, longitude et feat code

# In[210]:

for c in dataPark['features']:
    if 'park_name' in c['properties']:   
        park.insert_one({'parkName': c['properties']['park_name'],'area': c['properties']['shape_area'], 'latitude' : c['geometry']['coordinates'][0][0][0][1], 'longitude': c['geometry']['coordinates'][0][0][0][0], 'feat_code' : c['properties']['feat_code']})
    else:
        park.insert_one({'area': c['properties']['shape_area'],'latitude' : c['geometry']['coordinates'][0][0][0][1], 'longitude': c['geometry']['coordinates'][0][0][0][0], 'feat_code' : c['properties']['feat_code']})


# Insertion des données du métro. On garde les champs latitude, longitude, nom de la station et lignes desservies

# In[211]:

for c in datSubway:
    sub.insert_one({'latitude' : c['the_geom']['coordinates'][1], 'longitude': c['the_geom']['coordinates'][0], 
                    'name' : c['name'], 'line' : c['line'] })
    


# Les données concernant les piscines sont au format xml. 
# On utilise donc minidom.parseString pour traiter ces données

# In[48]:

doc = minidom.parseString(dataPool.content)
root = doc.documentElement


# Et ensuite on parcourt notre arbre en inserant les données au fur et à mesure

# In[53]:

for n in root.childNodes:
    if (n.hasChildNodes()):
        name = n.getElementsByTagName('Name')[0].childNodes[0].nodeValue
        lat = n.getElementsByTagName('lat')[0].childNodes[0].nodeValue
        long = n.getElementsByTagName('lon')[0].childNodes[0].nodeValue
        pool.insert_one({'poolName': name, 'latitude' : lat, 'longitude' : long})

