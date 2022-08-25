#
# Assignment5 Interface
# Name: Aditi Shashank Joshi (1222838916)
#

from pymongo import MongoClient
import os
import sys
import json
import re
import math

def calculate_distance(latitude1, longitude1, myLocation):
    latitude2=float(myLocation[0])
    longitude2=float(myLocation[1])

    R = 3959

    phi_1 = math.radians(latitude1)
    phi_2 = math.radians(latitude2)

    delta_omega = math.radians(latitude2-latitude1)
    delta_lamda = math.radians(longitude2-longitude1)

    a = math.sin(delta_omega/2) * math.sin(delta_omega/2) +math.cos(phi_1) * math.cos(phi_2) *math.sin(delta_lamda/2) * math.sin(delta_lamda/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c

    return d

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    city_file = open(saveLocation1,'w')
    for data in collection.find():
        if data['city'].lower()==cityToSearch.lower():
            city_file.write(data['name'].upper()+'$'+data['full_address'].upper()+'$'+data['city'].upper()+'$'+data['state'].upper()+'\n')
    city_file.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    location_file = open(saveLocation2,'w')
    for data in collection.find():
        if calculate_distance(data['latitude'],data['longitude'],myLocation)<=maxDistance:
            for category_data in categoriesToSearch:
                if category_data in data['categories']:
                    location_file.write(data['name'].upper()+'\n')
                    break;                   
    location_file.close()