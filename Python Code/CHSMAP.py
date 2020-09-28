#For Initial Data import

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

    items = sheet.get_all_values()

    jsonobject = {
        "type": "FeatureCollection",
        "name": "Points_2",
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        },
        "features": []
    }

    for item in items:
        if item == items[0]:
            continue
        if item[2] != "":
            search = geocoder.get(item[2])
            new = {
                "type": "Feature",
                "properties": {
                    "Hospital": item[0],
                    "City": re.sub(",.*$", "", item[1]),
                    "State": re.sub("^.*, ", "", item[1]),
                    "Address": item[2],
                    "POC": re.sub("\\n", ", ", item[3]),
                    "Status": item[4],
                    "Notes": item[5],
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
            jsonobject['features'].append(new)

    print("var json_Points_2 = " + json.dumps(jsonobject))
    with gcsfs.GCSFileSystem(project="hireclix").open("chs_map/Points_2.js", "w") as fl:
        fl.write("var json_Points_2 = " + json.dumps(jsonobject))
        fl.close()


CHSMAP()
