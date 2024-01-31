import csv
import geopy.distance
import pymysql
import sys
import os
import re
from decimal import Decimal

if "tadao" in sys.argv:
    os.chdir("gtfs-tadao-janvier-2023")
    raise Exception("regex_stop_id todo")

class Stop:
    def __init__(self, stopId, stopCode, stopName, stopLon, stopLat, parentStation):
        self.stopId = stopId
        self.stopCode = stopCode
        self.stopName = stopName
        self.stopLon = Decimal(stopLon)
        self.stopLat = Decimal(stopLat)
        self.parentStation = parentStation if parentStation else None
        
        
class Station:
    def __init__(self, stationId, stationCode, stationName, stationLon, stationLat):
        self.stationId = stationId
        self.stationCode = stationCode
        self.stationName = stationName
        self.stationLon = Decimal(stationLon)
        self.stationLat = Decimal(stationLat)
        
        
class Route:
    def __init__(self, routeId, agencyId, routeShortName, routeLongName, routeType, routeColor, routeTextColor):
        self.routeId = routeId
        self.agencyId = agencyId
        self.routeShortName = routeShortName
        self.routeLongName = routeLongName
        self.routeType = int(routeType)
        self.routeColor = int(routeColor, 16)
        self.routeTextColor = int(routeTextColor, 16)
        
        self.tripsNoDup = set()
        
        
class Trip:
    def __init__(self, routeId, serviceId, tripId, tripHeadsign, directionId):
        self.routeId = routeId
        self.serviceId = serviceId
        self.tripId = tripId
        self.tripHeadsign = tripHeadsign
        self.directionId = directionId
        
        self.stopTimes = []
        
        
class StopTime:
    def __init__(self, tripId, arrivalTime, departureTime, stopId, stopSequence):
        self.tripId = tripId
        self.arrivalTime = arrivalTime
        self.departureTime = departureTime
        self.stopId = stopId
        self.stopSequence = int(stopSequence)
        
        
class Site:
    def __init__(self, siteId, siteName, siteLat, siteLon):
        self.siteId = siteId
        self.siteName = siteName
        self.siteLat = siteLat
        self.siteLon = siteLon
        
        self.stops = {}
        
        
get_indexes = lambda keys, names: [keys.index(name) for name in names]
get_items = lambda line, keys: [line[index] for index in keys]

routes = {}
stops = {}
stations = {}
trips = {}

sites = {}

print("Connecting to database...")
conn = pymysql.connect(
    host="ssh.nathaan.com",
    user="user",
    password="pwd",
    database="olympics",
    port=3306,
    autocommit=True
)

cursor = conn.cursor()

print("Reading stops...")
with open("stops.txt", "r", encoding="utf-8") as f:
    reader = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    keys = get_indexes(next(reader), ["stop_id", "stop_code", "stop_name", "stop_lon", "stop_lat", "location_type", "parent_station"])
    
    for line in reader:
        stopId, stopCode, stopName, stopLon, stopLat, locationType, parentStation = get_items(line, keys)
        locationType = int(locationType)
        if locationType == 0:
            stop = Stop(stopId, stopCode, stopName, stopLon, stopLat, parentStation)
            stops[stop.stopId] = stop
            
        elif locationType == 1:
            station = Station(stopId, stopCode, stopName, stopLon, stopLat)
            stations[station.stationId] = station
            
        # on ignore le reste


print("Reading sites (from database)...")
cursor.execute("SELECT id, nom, latitude, longitude FROM sites WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
for siteId, siteName, siteLat, siteLon in cursor.fetchall():
    sites[siteId] = Site(siteId, siteName, siteLat, siteLon)
    
    
print("Calculating stops for sites...")
for siteId, site in sites.items():
    print("Site %d - %s" % (siteId, site.siteName))
    for stop in stops.values():
        dist = geopy.distance.great_circle((stop.stopLat, stop.stopLon), (site.siteLat, site.siteLon))
        if dist.m < 500:
            print("   %.2f m -> [%s] %s" % (dist.m, stop.stopId, stop.stopName))
            site.stops[stop.stopId] = dist.m

print("Reading routes...")
with open("routes.txt", "r", encoding="utf-8") as f:
    reader = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    keys = get_indexes(next(reader), ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"])
    
    for line in reader:
        route = Route(*get_items(line, keys))
        routes[route.routeId] = route
        
print("Reading trips...")
with open("trips.txt", "r", encoding="utf-8") as f:
    reader = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    keys = get_indexes(next(reader), ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"])
    
    for line in reader:
        trip = Trip(*get_items(line, keys))
        trips[trip.tripId] = trip
        
print("Reading stop_times... ", end="")
with open("stop_times.txt", "r", encoding="utf-8") as f:
    reader = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    keys = get_indexes(next(reader), ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])
    
    for index, line in enumerate(reader):
        if index & 0xFFF == 0:
            print(index * 100 // 9200000, end="%\rReading stop_times... ")
            
        stopTime = StopTime(*get_items(line, keys))
        trips[stopTime.tripId].stopTimes.append(stopTime)
        
print()
        
print("Sorting trips...")
for trip in trips.values():
    trip.stopTimes.sort(key = lambda x: x.stopSequence)
    

print("Simplifying routes trips... ", end="")
for index, trip in enumerate(trips.values()):
    if index & 0xFFF == 0:
        print(index * 100 // len(trips), end="%\rSimplifying routes trips... ")
        
    tripStops = tuple(stopTime.stopId for stopTime in trip.stopTimes)
    tripTup = (trip.tripHeadsign, tripStops)
    
    route = routes[trip.routeId]
    route.tripsNoDup.add(tripTup)
    
print()

cursor.execute("DELETE FROM trajets_arrets")
cursor.execute("DELETE FROM trajets")
cursor.execute("ALTER TABLE trajets AUTO_INCREMENT = 1")
cursor.execute("DELETE FROM itineraires") # routes
cursor.execute("DELETE FROM sites_arrets")
cursor.execute("DELETE FROM arrets")
cursor.execute("DELETE FROM agences")

print("Reading agencies...")
with open("agency.txt", "r", encoding="utf-8") as f:
    reader = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    keys = get_indexes(next(reader), ["agency_id", "agency_name"])
    
    for line in reader:
        agencyId, agencyName = get_items(line, keys)
        cursor.execute("INSERT INTO agences (id, nom) VALUES (%s, %s)", (agencyId, agencyName))
        
        
sitesStops = set()
for site in sites.values():
    sitesStops.update(site.stops)
    
print("Inserting interesting trips and stops to database...", end="")

for n, route in enumerate(routes.values()):
    if n & 0xF == 0:
        print(n * 100 // len(routes), end="%\rInserting interesting trips and stops to database... ")
        
    routeAdded = False
    
    for tripHeadsign, tripStops in route.tripsNoDup:
        # does it contain one of our stops
        if any(stop in sitesStops for stop in tripStops):
            if not routeAdded:
                cursor.execute("INSERT INTO itineraires (id, agence, nom_court, nom_long, type, couleur, couleur_texte) VALUES (%s, %s, %s, %s, %s, %s, %s)", (route.routeId, route.agencyId, route.routeShortName, route.routeLongName, route.routeType, route.routeColor, route.routeTextColor))
                routeAdded = True
                
            cursor.execute("INSERT INTO trajets (itineraire, titre) VALUES (%s, %s)", (route.routeId, tripHeadsign))
            rowId = cursor.lastrowid
            
            for index, stopId in enumerate(tripStops):
                stop = stops[stopId]
                cursor.execute("INSERT INTO arrets (id, nom, latitude, longitude, station) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id", (stop.stopId, stop.stopName, stop.stopLat, stop.stopLon, stop.parentStation))
                
                if stop.parentStation:
                    station = stations[stop.parentStation]
                    cursor.execute("INSERT INTO stations (id, nom, latitude, longitude) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id", (station.stationId, station.stationName, station.stationLat, station.stationLon))
                    
                cursor.execute("INSERT INTO trajets_arrets (trajet, position, arret) VALUES (%s, %s, %s)", (rowId, index, stopId))
        
for site in sites.values():
    for stopId, stopDist in site.stops.items():
        cursor.execute("INSERT INTO sites_arrets (site, arret, distance) VALUES (%s, %s, %s)", (site.siteId, stopId, stopDist))
    
print()
