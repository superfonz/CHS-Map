#For Automated Map connection

import json
import requests
import gspread
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os
from googlegeocoder import GoogleGeocoder
import re
import gcsfs


os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def CHSMAP():

    storage_client = storage.Client()

    GeocodeToken = json.loads(storage_client.get_bucket("hc_tokens_scripts")
                              .get_blob("Tokens/Google Keys.json")
                              .download_as_string())['Keys'][0]["Key"]

    geocoder = GoogleGeocoder(GeocodeToken)

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    credraw = storage_client.get_bucket('hc_tokens_scripts').blob(
        'Tokens/hireclix-googlesheets.json').download_as_string()

    credjson = json.loads(credraw)

    cred = ServiceAccountCredentials.from_json_keyfile_dict(credjson, scope)

    gclient = gspread.authorize(cred)

    sheet = gclient.open_by_key('1QelQJlQBTEWC_YY_t0Zmh7CIfn1WG714DqidJ3TW5Rw').worksheet('CHS Expansion Status')

    newpoints = sheet.get_all_values()

    with gcsfs.GCSFileSystem(project="hireclix").open("chs_map/Points_2.js", "r") as fl:
        prevpointsraw = fl.read()[20:]


    prevpoints = json.loads(prevpointsraw)

    newpointsinter = iter(newpoints)
    next(newpointsinter)

    for newpoint in newpointsinter:
        flag = 0
        if newpoint[0] == "" or newpoint[2] == "":
            continue
        for prevpoint in prevpoints['features']:
            if prevpoint['properties']['Hospital'] == newpoint[0]:
                prevpoint['properties']['POC'] = re.sub("\\n", ", ", newpoint[3])
                prevpoint['properties']['Status'] = newpoint[4]
                prevpoint['properties']['Notes'] = newpoint[5]
                if prevpoint['properties']['Address'] != newpoint[2]:
                    search = geocoder.get(newpoint[2])
                    prevpoint['properties']['City'] = re.sub(",.*$", "", newpoint[1])
                    prevpoint['properties']['State'] = re.sub("^.*, ", "", newpoint[1])
                    prevpoint['properties']['Address'] = newpoint[2]
                    prevpoint['properties']['Latitude'] = search[0].geometry.location.lat
                    prevpoint['properties']['Longitude'] = search[0].geometry.location.lng
                    prevpoint['geometry']['coordinates'] = [search[0].geometry.location.lng,
                                                            search[0].geometry.location.lat]
                flag = 1
                break
        if flag == 1:
            continue
        elif flag == 0:
            search = geocoder.get(newpoint[2])
            new = {
                "type": "Feature",
                "properties": {
                    "Hospital": newpoint[0],
                    "City": re.sub(",.*$", "", newpoint[1]),
                    "State": re.sub("^.*, ", "", newpoint[1]),
                    "Address": newpoint[2],
                    "POC": re.sub("\\n", ", ", newpoint[3]),
                    "Status": newpoint[4],
                    "Notes": newpoint[5],
                    "Latitude": search[0].geometry.location.lat,
                    "Longitude": search[0].geometry.location.lng
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        search[0].geometry.location.lng,
                        search[0].geometry.location.lat
                    ]
                }
            }

            prevpoints['features'].append(new)

    print(prevpoints)
    raise KeyboardInterrupt
    with gcsfs.GCSFileSystem(project="hireclix").open("chs_map/Points_2.js", "w") as fl:
        fl.write("var json_Points_2 = " + json.dumps(prevpoints))
        fl.close()

CHSMAP()
