# GTFStoExcel
import needed : 
  sqlite3
  os
  csv
  openpyxl

architecture of the dictionary :
{ route_id : [{(trip_id, service_id) : [(stops.stop_id, stop_times.departure_time),..., (stops.stop_id, stop_times.departure_time)]},
              ...
              {(trip_id, service_id) : [(stops.stop_id, stop_times.departure_time),..., (stops.stop_id, stop_times.departure_time)]}],
 ...
  route_id : [{(trip_id, service_id) : [(stops.stop_id, stop_times.departure_time),..., (stops.stop_id, stop_times.departure_time)]},
              ...
              {(trip_id, service_id) : [(stops.stop_id, stop_times.departure_time),..., (stops.stop_id, stop_times.departure_time)]}]}
