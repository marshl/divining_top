from optparse import OptionParser
import urllib.request
import psycopg2
import zipfile
import json
import requests
from os import path

dataFolder = '../data'

parser = OptionParser()

parser.add_option("-c", "--connection", dest="connection_string",
                  help="The postgres connection string")

parser.add_option("-d", "--download", action="store_true", dest="download",
                  help="downloads the json file even if it already exists")

(options, args) = parser.parse_args()

jsonZip = path.join( dataFolder, 'AllSets-x.json.zip' )
jsonFile = path.join( dataFolder, 'AllSets-x.json' )
jsonPrettyFile = path.join( dataFolder, 'AllSets-x-pretty.json' )

def main():
    if options.download or not path.isfile(jsonFile):
        download_json_data()

    json_data = parse_json_data()

    conn = connect_to_database()

    update_block_information(json_data, conn)
    update_set_information(json_data, conn)

def parse_json_data():
    f = open( jsonFile, 'r', encoding="utf8" )
    json_data = json.load( f, encoding='UTF-8' )
    f.close()

    json_data = sorted( json_data.items(), key=lambda set: set[1]["releaseDate"] )

    return json_data

def download_json_data():
      
    url = "http://mtgjson.com/json/AllSets-x.json.zip"
    r = requests.get(url)

    with open(jsonZip,'wb') as output:
        output.write(r.content)
    
    jsonZip = zipfile.ZipFile(jsonZip)
    jsonZip.extractall(dataFolder)



def connect_to_database():
    
    conn = psycopg2.connect(options.connection_string)
    return conn

def update_block_information(json_data, connection):
    
    print( "Updating block information... " )

    cursor = connection.cursor()

    for set in json_data:

        if 'block' not in set[1]:
            continue

        cursor.execute(
        """INSERT INTO spellbook_block (name, release_date)
           VALUES (%s, %s)
           ON CONFLICT (name) DO NOTHING""",
        (set[1]['block'], set[1]['releaseDate']))

    cursor.close()

    print( "Done\n" )

def update_set_information(json_data, connection):
    
    print( "Updating set information... ")
    cursor = connection.cursor()

    for set in json_data:
        
        cursor.execute(
        """INSERT INTO spellbook_set (code, name, release_date, block_id)
           VALUES (%s, %s, %s, (SELECT id FROM spellbook_block WHERE name = %s) )
           ON CONFLICT (code) DO NOTHING""",
        (set[0], set[1]['name'], set[1]['releaseDate'], set[1].get('block') ))

    connection.commit()
    cursor.close()

    print( "Done\n" )

if __name__ == "__main__":
    main()
