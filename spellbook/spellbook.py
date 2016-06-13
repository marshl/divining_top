import urllib.request
import zipfile
import json
import requests
from os import path

dataFolder = '../data'
jsonZip = path.join( dataFolder, 'AllSets-x.json.zip' )
jsonFile = path.join( dataFolder, 'AllSets-x.json' )
jsonPrettyFile = path.join( dataFolder, 'AllSets-x-pretty.json' )
#urllib.request.urlretrieve( "http://mtgjson.com/json/AllSets-x.json.zip", jsonZip )



url = "http://mtgjson.com/json/AllSets-x.json.zip"
r = requests.get(url)

with open(jsonZip,'wb') as output:
    output.write(r.content)


jsonZip = zipfile.ZipFile(jsonZip)
jsonZip.extractall(dataFolder)

f = open( jsonFile, 'r', encoding="utf8" )
#data = f.read().decode('utf8')
jsonData = json.load( f, encoding='UTF-8' )
f.close()

#print( json.dumps( jsonData, sort_keys=True, indent=4, separators=(',', ': ') ) )

#f = open( jsonPrettyFile, 'w', encoding='utf8' )
#f.write( json.dumps( jsonData, sort_keys=True, indent=2, separators=(',', ':') ) )
#f.close()

# List of sets, ordered by release date
#setlist = [ set for set in jsonData ]

# Sort the set by release date
sortedSetList = sorted( jsonData, key=lambda set: jsonData[set]["releaseDate"] )

#for set in jsonData:
#    print( dateutil.parser.parse( jsonData[set]["releaseDate"] ).toordinal() )

#print( sortedSetList )

for setcode in sortedSetList:
    #print( set["code"] )
    print( jsonData[setcode]['code'] )
    set = jsonData[setcode]

    if len( set['cards'] ) == 0:
        continue;

    sortedCards = set['cards']

    if 'number' in sortedCards[0]:
        sortedCards.sort( key = lambda card: card['number'] )
    elif 'multiverseid' in sortedCards[0]:
        sortedCards.sort( key = lambda card: card['multiverseid'] )
    else:
        sortedCards.sort( key = lambda card: card['name'] )
    
    #sorted( set['cards'], key = lambda card: card['number'] if ( 'number' in card ) else card['multiverseid'] if ( 'multiverseid' in card ) else card['name'])
    for card in sortedCards:
        print( card['name'] )