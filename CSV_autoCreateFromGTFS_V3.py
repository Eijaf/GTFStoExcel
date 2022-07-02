# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 21:34:30 2022

@author: Adrien SCHWEGLER
"""
import csv, sqlite3,os
from openpyxl import load_workbook, Workbook
from copy import deepcopy


def extratDataFromFile(fileName, columnName): 
     with open(fileName, 'r', encoding='utf-8_sig') as file:
            dr = csv.DictReader(file)
            table_info = [tuple([i[column] for column in columnName]) for i in dr]
     return table_info
 
    
def createDataBaseTableFromCsv(csvfile):
    indexStops, indexDirection = -1, -1  #réinitialisation de l'index
        
    #Récupèration des colones du fichier
    with open(csvfile, 'r', encoding='utf-8_sig') as file:
        listColumn = file.readline().rstrip()      #first line of the file
        columnName = listColumn.split(",")          #from str to list

    
    table_info = extratDataFromFile(csvfile, columnName)  #contenue de la table
    print('        -Information de la table récupérée')

    
    numberOfColumn = len(columnName)  #nombre de colone    
    createSintax = listColumn   #str for insertion in CREATE sentence
    #add INTERGER when needed
    indexStops = createSintax.find('stop_sequence')
    indexDirection = createSintax.find('direction_id')
    if indexStops != -1:
        createSintax = createSintax[:indexStops+13] + ' INTERGER ' + createSintax[indexStops+13:]
    if indexDirection != -1:
        createSintax = createSintax[:indexDirection+12] + ' INTERGER ' + createSintax[indexDirection+12:]
    #create and fill table
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE {} ({});".format(csvfile[:-4], createSintax))
    cursor.executemany("INSERT INTO {} ({}) VALUES ({});".format(csvfile[:-4], listColumn, ('?,'*numberOfColumn)[:-1]), table_info)  #Outch
    connection.commit()

def extractTrip(dictRoutes, direction):
    dictTrip = {}
    for route, tripe in dictRoutes.items():
        cursor = connection.cursor()
        cursor.execute('''SELECT trips.trip_id
                       FROM trips
                       JOIN stop_times
                       ON stop_times.trip_id = trips.trip_id
                       WHERE trips.route_id = "{}" AND trips.direction_id = "{}"
                       ORDER BY stop_times.departure_time
                        '''.format(route, direction))
        dictTrip[route] = {key[0] : {} for key in cursor}
    return dictTrip


def suppdoubleAndExtractStops(dictTrip): #from trips_id
    dictTripStops = deepcopy(dictTrip)
    for route_id, value in dictTrip.items():
        compare = []                #list of all trips number already pass(not necessarly same day)
        for trip_id, noth in value.items():
            index_end_number = 0   
            for caract in trip_id:      #looking for the len of the trips's number at the begining of the trip_id
                if 48<=ord(caract)<=57: #if it is a number
                    index_end_number += 1 #add one to the index of the end
                else:
                    break#if not : the number has end so break the loop            
            if trip_id[:index_end_number] in compare:
                del dictTripStops[route_id][trip_id]
            else :
               cursor = connection.cursor()
               cursor.execute('''SELECT stops.stop_id, stop_times.departure_time
                              FROM stops
                              JOIN stop_times
                              ON stop_times.stop_id = stops.stop_id 
                              WHERE stop_times.trip_id = "{}" AND stop_times.drop_off_type !="1"
                              ORDER BY stop_times.stop_sequence'''.format(trip_id))
               for stops in cursor.fetchall():
                   dictTripStops[route_id][trip_id][stops[0]] = stops[1][:5]
               compare.append(trip_id[:index_end_number])
    return dictTripStops   


def extractStops(route, direction): #from route_id
    cursor = connection.cursor()
    cursor. execute('''SELECT stops.stop_id ,stops.stop_name 
                    FROM stops
                    JOIN stop_times
                    ON stop_times.stop_id = stops.stop_id
                    JOIN trips
                    ON trips.trip_id = stop_times.trip_id
                    WHERE trips.route_id = "{}" AND trips.direction_id = {} AND stop_times.drop_off_type !="1"
                    GROUP BY stops.stop_id
                    ORDER BY stop_times.stop_sequence ASC'''.format(route, direction))
    return {row[0] : row[1] for row in cursor} 


def createTable(route, dictStops, dictSens) :
    table = [[stops] for stops in dictStops]
    dictRoute = dictSens[route]    
    for trip, stops in dictRoute.items():
        for i in range(len(dictStops)):
            if table[i][0] in stops:
                
                table[i].append(stops[table[i][0]])
            else :
                table[i].append('-')
    for stops in table: #je change le stop_id pour le stop_name
        stops[0] = dictStops[stops[0]]
    return table            

    
def createCSV(dictSens0, dictSens1):
    for route in listRoutes:
        fileName = route + '.csv' 
        dictStops0 = extractStops(route, 0) 
        dictStops1 = extractStops(route, 1)
        table0 = createTable(route, dictStops0, dictSens0) #grille sens 0
        table1 = createTable(route, dictStops1, dictSens1) #grille sens 1
        with open(fileName,'w', newline = '', encoding='utf-8') as file:
            obj = csv.writer(file, delimiter = ",")
            for element in table0:  #on écrit le sens 0
                obj.writerow(element)
            obj.writerow('\n')
            for element in table1:  #on écrit le sens 1
                obj.writerow(element)


def createXLS(listRoutes, dictSens0, dictSens1):
    wb = Workbook()
    wb.save('Horraires_GTFS.xlsx')
    wb = load_workbook('Horraires_GTFS.xlsx')
    sheet = wb.active
    sheet.title = 'Informations'
    sheet['A1'] = 'Ce fichier contient les routes :'
    for element in listRoutes:
         sheet.append([element])
    compteur = 1
    nb_routes = len(listRoutes)
    for route in listRoutes : #création des feuilles
        print('    -Feuille {} sur {}.'.format(compteur, nb_routes))
        dictStops0 = extractStops(route, 0) 
        dictStops1 = extractStops(route, 1)
        table0 = createTable(route, dictStops0, dictSens0) #grille sens 0
        table1 = createTable(route, dictStops1, dictSens1) #grille sens 1
        wb.create_sheet(title=route)
        sheet = wb[route]
        for i in table0:
            sheet.append(i) 
        sheet.append([]) 
        for i in table1:
            sheet.append(i) 
        compteur +=1
    wb.save('Horraires_GTFS.xlsx')

try:
    print('Création de la base de données...')
    connection = sqlite3.connect('BD_GTFS.db')
    print('Connection à la base de donnée établie.')
    print('-'*50)

    print('Récupération des fichiers et créations des tables...')
    necessaryFiles = ['routes.txt', 'stop_times.txt', 'stops.txt','trips.txt', 'calendar.txt']
    
    
    #Creation Tables of data_base
    for csvfile in necessaryFiles :
        print('   -Table {}...'.format( csvfile[:-4]))
        createDataBaseTableFromCsv(csvfile)
        print('        -Table {} crée'.format(csvfile[:-4]))
        
    print('-'*50)    
    print('Récupération des routes...')
    cursor = connection.execute("SELECT route_id FROM routes") #Je récupère la liste de toutes les routes
    listRoutes = [row[0] for row in cursor]
    print('Routes trouvées : ',listRoutes)
    
    print('-'*50)
    print('Création des dictionnaires...')
    dictRoutes = {key : {} for key in listRoutes}
    print('    -Etape 1/4...')
    dictTrip0 = extractTrip(dictRoutes, 0) #j'extrait tout les voyages par ligne dans le sens 0
    print('    -Etape 2/4...')
    dictTrip1 = extractTrip(dictRoutes, 1) #j'extrait tout les voyages par ligne dans le sens 1
    print('    -Etape 3/4...')
    dictTripStops0 = suppdoubleAndExtractStops(dictTrip0)
    print('    -Etape 4/4...')
    dictTripStops1 = suppdoubleAndExtractStops(dictTrip1)

    print('-'*50)
    print('Création des fichiers .csv...')
    createCSV(dictTripStops0, dictTripStops1)
    print('-'*50)
    print('Création du fichier .xlsx...')
    createXLS(listRoutes, dictTripStops0, dictTripStops1)
    print('-'*50)
    print("L'oppération c'est déroulée correctement")
    
except sqlite3.Error as error:
    print('Une erreur est survenue -', error)
    
except FileNotFoundError:
    print('Erreur ! Fichier "{}" manquant.'.format(csvfile))

except KeyboardInterrupt:
    print("Arret de l'oppération")

finally:
    if connection :
        connection.close()
        print('La connection à la base de donnée à été fermée.')    
    os.system("pause")