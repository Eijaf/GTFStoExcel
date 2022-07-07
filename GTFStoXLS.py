# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 21:34:30 2022

@author: Adrien SCHWEGLER
"""

import csv, sqlite3, os, openpyxl
from copy import deepcopy

def extratDataFromFile(fileName, columnName):
     with open(fileName, 'r', encoding='utf-8_sig') as file:
            dr = csv.DictReader(file)
            table_info = [tuple([i[column] for column in columnName]) for i in dr]
     return table_info

def createDataBaseTableFromCsv(csvfile):
    print('   -Table {}...'.format( csvfile[:-4]))
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
    cursor.execute("CREATE TABLE {} ({});".format(csvfile[:-4], createSintax))
    cursor.executemany("INSERT INTO {} ({}) VALUES ({});".format(csvfile[:-4], listColumn, ('?,'*numberOfColumn)[:-1]), table_info)  #Outch
    connection.commit()
    print('        -Table {} crée'.format(csvfile[:-4]))

def extractTrip(listRoutes, direction):
    dictTrip = {}
    for route in listRoutes:
        cursor.execute('''SELECT trips.trip_id, trips.service_id, trips.route_id
                       FROM trips
                       JOIN stop_times
                       ON stop_times.trip_id = trips.trip_id
                       JOIN routes
                       ON routes.route_id = trips.route_id
                       WHERE routes.route_short_name = "{}" AND trips.direction_id = "{}"
                       ORDER BY stop_times.departure_time
                        '''.format(route, direction))
        dictTrip[route] = {key : {} for key in cursor}
    return dictTrip

def findTripNumber(trip_id): #looking for the len of the trips's number at the begining of the trip_id
    index_end_number = 0   
    for caract in trip_id:
        if 48<=ord(caract)<=57: #if it is a number
            index_end_number += 1 #add one to the index of the end
        else:
            break#if not : the number has end so break the loop
    return trip_id[:index_end_number] 

def GetstrDays(service_id):
    cursor.execute('''SELECT monday, tuesday, wednesday, thursday, friday, saturday, sunday
                   FROM calendar
                   WHERE service_id = "{}"'''.format(service_id))
    listDays = cursor.fetchall()[0]
    strday = ''
    week = ['L', 'Ma', 'Me', 'J', 'V', 'S', 'D']
    for i in range(7):
        if int(listDays[i]):
            strday +=  week[i]
    return strday

def GetstrPeriode(service_id) :
    cursor.execute('''SELECT start_date, end_date
                   FROM calendar
                   WHERE service_id = "{}"'''.format(service_id))
    periodBrut =  cursor.fetchall()[0]
    start, end = periodBrut[0], periodBrut[1]
    return (start[-2:] + '/'+ start[4:6]+ '/' + start[:4], end[-2:] + '/'+end[4:6]+ '/' + end[:4])   #Change date format

def rewriteStrDAys(dictDaysTrips):
    week = ['L', 'Ma', 'Me', 'J', 'V', 'S', 'D'] 
    for trips, strday0 in dictDaysTrips.items():
        strday = ''
        for day in week:    #right order (case of : JVLMa to LMaJV)
            if day in strday0:
                strday += day  
        #better
        if strday == 'LMaMeJV':
            dictDaysTrips[trips] = 'LàV'
        elif strday == 'LMaMeJVS':
            dictDaysTrips[trips] = 'LàS'
        elif strday == 'Me':
            dictDaysTrips[trips] = 'Mer'
        else:
            dictDaysTrips[trips] = strday
    return dictDaysTrips

def suppdoubleAndExtractStops(dictTrip, listRoute): 
    dictTripStops = deepcopy(dictTrip)
    dicDays = {}
    dicPeriode = {}
    for route_short, value in dictTrip.items():
        compare = []                #list of all trips number already pass(not necessarly same day)
        dictDaysTrips = {}
        dictPeriodeTrips = {}
        for tripAndService_id, nothing in value.items():
            trip_id = tripAndService_id[0]
            service_id =  tripAndService_id[1]
            tripNumber = findTripNumber(trip_id)                     
            if tripNumber in compare:
                dictDaysTrips[tripNumber] += GetstrDays(service_id) #+= cause already exist                
                del dictTripStops[route_short][tripAndService_id] #delet the double
            else :
               cursor.execute('''SELECT stops.stop_id, stop_times.departure_time, stop_times.pickup_type
                              FROM stops
                              JOIN stop_times
                              ON stop_times.stop_id = stops.stop_id 
                              WHERE stop_times.trip_id = "{}" AND stop_times.drop_off_type !="1"
                              ORDER BY stop_times.stop_sequence'''.format(trip_id))
               for stops in cursor.fetchall():
                   dictTripStops[route_short][tripAndService_id][stops[0]] = stops[1][:5] + ' ✆' if stops[2]=='2' else stops[1][:5]
               dictDaysTrips[tripNumber] = GetstrDays(service_id)
               dictPeriodeTrips[tripNumber] = GetstrPeriode(service_id)
               compare.append(tripNumber)

        dictDaysTrips = rewriteStrDAys(dictDaysTrips) #on réécrit le cat régulier des jours
        dicDays[route_short] = dictDaysTrips
        dicPeriode[route_short] = dictPeriodeTrips
    return dictTripStops, dicDays, dicPeriode

def extractStops(route, direction): #retrun the stops in the right order
    cursor. execute('''SELECT stops.stop_id ,stops.stop_name 
                    FROM stops
                    JOIN stop_times
                    ON stop_times.stop_id = stops.stop_id
                    JOIN trips
                    ON trips.trip_id = stop_times.trip_id
                    JOIN routes
                    ON routes.route_id = trips.route_id
                    WHERE routes.route_short_name = "{}" AND trips.direction_id = '{}' AND stop_times.drop_off_type !="1"
                    GROUP BY stops.stop_id HAVING MIN(stop_times.departure_time)
                    ORDER BY stop_times.departure_time ASC'''.format(route, direction))          
    return {row[0] : row[1] for row in cursor}

def createTable(route, dictStops, dictSens, dicDayTrip, dicPeriodTrip) :
    table = [ ['Début de validité'], ['Fin de validité'],[''], ['Jours de circulation'],['']] + [[stops] for stops in dictStops]
    dictRoute = dictSens[route]    
    for tripAndService, stops in dictRoute.items():
        trip_id = tripAndService[0]
        tripNumber = findTripNumber(trip_id)        
        table[0].append(dicPeriodTrip[tripNumber][0]) #début de validité
        table[1].append(dicPeriodTrip[tripNumber][1]) #fin de validité
        table[2].append(tripAndService[2]) #route_id for information of 'ad'
        table[3].append(dicDayTrip[tripNumber]) #jours de passage du voyage
        for i in range(5, len(dictStops)+5):     #heure de passage du voyage
            if table[i][0] in stops: 
                table[i].append(stops[table[i][0]])
            else :
                table[i].append('-')
	
    for stops in table: #change stop_id for the matching stop_name
        try :
            stops[0] = dictStops[stops[0]]
        except:  #to avoid key problem when at 'jour de validité'
            pass
    return table

def getRouteName(route):
    cursor.execute('''SELECT route_short_name, route_long_name
                   FROM routes
                   WHERE route_short_name = "{}"'''.format(route))
    return cursor.fetchall()[0]

def getRouteColor(route):
    cursor.execute('''SELECT route_color
                       FROM routes
                       WHERE route_short_name = "{}"'''.format(route))
    return cursor.fetchall()[0][0]

def createXLS(listRoutes, dictSens0, dictSens1,dicDays0, dicDays1, dicPeriodRoute0, dicPeriodRoute1):
    wb = openpyxl.Workbook()
    wb.save('Horaires_GTFS.xlsx')
    wb = openpyxl.load_workbook('Horaires_GTFS.xlsx')
    
    #Introduction Sheet
    sheet = wb.active
    sheet.title = 'Informations'
    sheet['A1'] = 'Ce fichier contient les routes :'
    for element in listRoutes:
        sheet.append([element])
        
        
    ad_style = openpyxl.styles.NamedStyle(name = 'ad_style')
    ad_style.fill = openpyxl.styles.PatternFill(patternType = 'solid', fgColor = 'DDDDDD')

    compteur = 1
    nb_routes = len(listRoutes)
    for route in listRoutes : #création des feuilles
        print('    -Feuille {} sur {}.'.format(compteur, nb_routes))
        dictStops0 = extractStops(route, 0) 
        dictStops1 = extractStops(route, 1)
        dicDayTrip0, dicDayTrip1 = dicDays0[route], dicDays1[route]
        dicPeriodTrip0, dicPeriodTrip1 = dicPeriodRoute0[route], dicPeriodRoute1[route]
        table0 = createTable(route, dictStops0, dictSens0, dicDayTrip0, dicPeriodTrip0) #grille sens 0
        table1 = createTable(route, dictStops1, dictSens1, dicDayTrip1, dicPeriodTrip1) #grille sens 1

        wb.create_sheet(title=route)
        sheet = wb[route]
 
        routeName = getRouteName(route)
        sheet['A1'] = routeName[0] + ' - ' + routeName[1]   

        sheet.append([])

        for i in table0:
            sheet.append(i)
        sheet.append([]) 
        sheet.append([]) 
        for i in table1:
            sheet.append(i)

        #page layout
        iscolor = 0
        color = getRouteColor(route)
        try:
            sheet.sheet_properties.tabColor = color
            iscolor = 1
        except:
            pass
			
        for column in sheet.columns: 
            for cell in column:
                try:
                    if not cell.row % 2:
                        if '✆' in cell.value:
                            cell.style = ad_style
                        elif iscolor :
                            cell.fill = openpyxl.styles.PatternFill(patternType = 'lightDown', fgColor = color)
                except: #if celle empty : none so no iterable
                    pass
                if cell.column != 1:
                    cell.alignment = openpyxl.styles.Alignment(horizontal = 'center')

    if not iscolor:
        print('Couleur non renseignée pour la route {}'.format(route))
    compteur +=1
    wb.save('Horaires_GTFS.xlsx')

try:
    print('Création de la base de données...')
    connection = sqlite3.connect('BD_GTFS.db')
    cursor = connection.cursor()
    print('Connection à la base de donnée établie.')
    
    print('-'*50)

    print('Récupération des fichiers et créations des tables...')
    necessaryFiles = ['routes.txt', 'stop_times.txt', 'stops.txt','trips.txt', 'calendar.txt']
    for csvfile in necessaryFiles :
        createDataBaseTableFromCsv(csvfile)

    print('-'*50)   

    print('Récupération des routes...')
    cursor.execute("SELECT route_short_name FROM routes GROUP BY route_short_name") #Je récupère la liste de toutes les routes
    listRoutes = [row[0] for row in cursor]
    print('Routes trouvées : ',listRoutes)

    print('-'*50)

    print('Création des dictionnaires...')
    print('    -Etape 1/4...')
    dictTrip0 = extractTrip(listRoutes, 0) #direction 0
    print('    -Etape 2/4...')
    dictTrip1 = extractTrip(listRoutes, 1) #direction 1
    print('    -Etape 3/4...')
    result0 = suppdoubleAndExtractStops(dictTrip0, listRoutes)
    dictTripStops0, dicDays0, dicPeriodRoute0 = result0[0], result0[1], result0[2]
    print('    -Etape 4/4...')
    result1 = suppdoubleAndExtractStops(dictTrip1, listRoutes)
    dictTripStops1, dicDays1, dicPeriodRoute1 = result1[0], result1[1], result1[2]

    print('-'*50)

    print('Création du fichier .xlsx...')
    createXLS(listRoutes, dictTripStops0, dictTripStops1, dicDays0, dicDays1, dicPeriodRoute0, dicPeriodRoute1)

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
