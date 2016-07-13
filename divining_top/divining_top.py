from optparse import OptionParser
import urllib.request
import psycopg2
import zipfile
import json
import requests
import re
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

colour_name_to_flag = {
    'white': 1,
    'blue': 2,
    'black': 4,
    'red': 8,
    'green': 16,
}

colour_code_to_flag = {
    'w': 1,
    'u': 2,
    'b': 4,
    'r': 8,
    'g': 16,
}

rarity_name_to_code = {
    'basic land': 'L',
    'common': 'C',
    'uncommon': 'U',
    'rare': 'R',
    'mythic rare': 'M'
}

def main():
    if options.download or not path.isfile(jsonFile):
        download_json_data()

    json_data = parse_json_data()
    
    # f = open( 'AllSets-x-pretty.json', 'w', encoding='utf8' )
    # f.write( json.dumps( json_data, sort_keys=True, indent=2, separators=(',', ':') ) )
    # f.close()
    
    #return

    conn = connect_to_database()

    update_rarity_table(conn)

    #update_block_information(json_data, conn)
    #update_set_information(json_data, conn)
    update_card_information(json_data, conn)

    conn.commit()
    conn.close()

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

def reset_database(connection):

    cursor = connection.cursor()

    cursor.execute("""
TRUNCATE spellbook_card CASCADE;
ALTER SEQUENCE spellbook_block_id_seq RESTART;
ALTER SEQUENCE spellbook_card_id_seq RESTART;
ALTER SEQUENCE spellbook_cardlink_id_seq RESTART;
ALTER SEQUENCE spellbook_cardprinting_id_seq RESTART;
ALTER SEQUENCE spellbook_cardprintinglanguage_id_seq RESTART;
ALTER SEQUENCE spellbook_cardruling_id_seq RESTART;
TRUNCATE spellbook_rarity CASCADE;
ALTER SEQUENCE spellbook_rarity_id_seq RESTART;
TRUNCATE spellbook_set CASCADE;
ALTER SEQUENCE spellbook_set_id_seq RESTART;
""")

    cursor.close()

def update_rarity_table(connection):
    cursor = connection.cursor()

    cursor.execute("""
INSERT INTO spellbook_rarity (
    symbol,
    name,
    display_order
) VALUES (
'L',
'Basic Land',
10
), (
'C',
'Common',
20
), (
'U',
'Uncommon',
30
), (
'R',
'Rare',
40
), (
'M',
'Mythic Rare',
50
), (
'T',
'Timeshifted',
60
), (
'S',
'Special',
70
)
ON CONFLICT(symbol) DO NOTHING;
""")

    cursor.close()

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

    cursor.close()

    print( "Done\n" )

def update_card_information(json_data, connection):

    print( "Updating card information... ", end="" )
    cursor = connection.cursor()

    for set in json_data:
        
        for card in set[1]['cards']:
            
            print( card['name'] )

            update_card(card, set[0], cursor)

    cursor.close()
    print( "Done." )

def update_card(card, setcode, cursor):

    card_colour = get_colour_flags_from_names(card['colors']) if card.get('colors') else 0

    card_details = {
        'cost': card.get('manaCost'),
        'cmc': card.get('cmc') or 0,
        'colour': card_colour,
        'colour_identity': get_colour_flags_from_codes(card['colorIdentity']) if card.get('colourIdentity') else 0,
        'colour_count': bin(card_colour).count('1'),
        'type': ' '.join( card.get('types') ) if card.get('types') else None,
        'subtype': ' '.join( card.get('subtypes') ) if card.get('subtypes') else None,
        'power': card.get('power'),
        'num_power': convert_to_number(card.get('power')) if card.get('power') else 0,
        'toughness': card.get('toughness'),
        'num_toughness': convert_to_number(card.get('toughness')) if card.get('toughness') else 0,
        'loyalty': card.get('loyalty'),
        'num_loyalty': convert_to_number(card.get('loyalty')) if card.get('loyalty') else 0,
        'rules_text': card.get('text')
    }

    cnum_match = re.search( '([\d.]+)(\w+)?', card['number'] ) if card.get('number') else None

    printing_details = {
        'rarity': 'Timeshifted' if card.get('timeshifted') and card['timeshifted'] else card.get('rarity'),
        'flavour_text': card.get('flavor'),
        'artist': card['artist'],
        'collector_number': cnum_match.groups()[0] if cnum_match else None,
        'collector_letter': cnum_match.groups()[1] if cnum_match and len(cnum_match.groups()) == 2 else None,
        'original_text': card.get('originalText'),
        'original_type': card.get('originalType'),
        'setcode': setcode
    }

    language_details = {
        'language': 'English',
        'card_name': card['name'],
    }

    # Find whether a card of the given name exists in the database yet
    cursor.execute("""
SELECT
  c.id card_id,
  cp.id printing_id,
  cpl.id language_id
FROM spellbook_card c
JOIN spellbook_cardprinting cp
  ON cp.card_id = c.id
JOIN spellbook_cardprintinglanguage cpl
  ON cpl.card_printing_id = cp.id
WHERE cpl.language = 'English'
AND cpl.card_name = %(name)s
LIMIT 1
""", {'name': card['name']});

    (card_id, printing_id, language_id) = cursor.fetchone() or (None, None, None)

    # If the card does not exist in the database, then the 
    if card_id is None:

        print('Card was not found, inserting new record')

        cursor.execute("""
INSERT INTO spellbook_card (
    cost,
    cmc,
    colour,
    colour_identity,
    colour_count,
    type,
    subtype,
    power,
    num_power,
    toughness,
    num_toughness,
    loyalty,
    num_loyalty,
    rules_text
) VALUES (
    %(cost)s,
    %(cmc)s,
    %(colour)s,
    %(colour_identity)s,
    %(colour_count)s,
    %(type)s,
    %(subtype)s,
    %(power)s,
    %(num_power)s,
    %(toughness)s,
    %(num_toughness)s,
    %(loyalty)s,
    %(num_loyalty)s,
    %(rules_text)s
)
""", card_details)
        cursor.execute("SELECT lastval()")
        rows = cursor.fetchone()
        card_id = rows[0]
        
        print('card_id is now {0}'.format(card_id))

        printing_details['card_id'] = card_id

#        cursor.execute("""
#INSERT INTO spellbook_cardprinting (
#    rarity_id,
#    flavour_text,
#    artist,
#    collector_number,
#    collector_letter,
#    original_text,
#    original_type,
#    card_id,
#    set_id
#) VALUES (
#    ( SELECT id FROM spellbook_rarity WHERE name = %(rarity)s ),
#    %(flavour_text)s,
#    %(artist)s,
#    %(collector_number)s,
#    %(collector_letter)s,
#    %(original_text)s,
#    %(original_type)s,
#    %(card_id)s,
#    ( SELECT id FROM spellbook_set WHERE code = %(setcode)s )
#)""", printing_details )

#        cursor.execute("SELECT lastval()")
#        rows = cursor.fetchone()
#        printing_id = rows[0]

#        print('printing_id is now {0}'.format(printing_id))

#        language_details['card_printing_id'] = printing_id

#        cursor.execute("""
#INSERT INTO spellbook_cardprintinglanguage (
#    language,
#    card_name,
#    card_printing_id
#) VALUES (
#    %(language)s,
#    %(card_name)s,
#    %(card_printing_id)s
#)""", language_details )

#        cursor.execute("SELECT lastval()")
#        rows = cursor.fetchone()
#        language_id = rows[0]

#        print('language_id is now {0}'.format(language_id))

    else: # card_id is not None

        print('Card already exists card_id:{0} printing_id:{1} language_id:{2}'.format(card_id, printing_id, language_id))

        card_details['card_id'] = card_id
        printing_details['card_id'] = card_id
        cursor.execute("""
UPDATE spellbook_card SET
    cost =  %(cost)s,
    cmc = %(cmc)s,
    colour = %(colour)s,
    colour_identity =  %(colour_identity)s,
    colour_count = %(colour_count)s,
    type = %(type)s,
    subtype =  %(subtype)s,
    power = %(power)s,
    num_power = %(num_power)s,
    toughness =  %(toughness)s,
    num_toughness = %(num_toughness)s,
    loyalty =  %(loyalty)s,
    num_loyalty = %(num_loyalty)s,
    rules_text = %(rules_text)s
WHERE id = %(card_id)s
""", card_details)


    cursor.execute("""
SELECT id
FROM spellbook_cardprinting
WHERE card_id = %(card_id)s
AND set_id = ( SELECT id FROM spellbook_set WHERE code = %(setcode)s )
AND collector_number IS NOT DISTINCT FROM %(collector_number)s
AND collector_letter IS NOT DISTINCT FROM %(collector_letter)s
""", { 'card_id': card_id, 'setcode': setcode, 'collector_number': printing_details['collector_number'], 'collector_letter': printing_details['collector_letter'] } )
    
    rows = cursor.fetchone()
    if rows is not None:
        printing_id = rows[0]
    else:
        printing_id = None

    if printing_id is None:

        print('card printing is new')

        cursor.execute("""
    INSERT INTO spellbook_cardprinting (
        rarity_id,
        flavour_text,
        artist,
        collector_number,
        collector_letter,
        original_text,
        original_type,
        card_id,
        set_id
    ) VALUES (
        ( SELECT id FROM spellbook_rarity WHERE name = %(rarity)s ),
        %(flavour_text)s,
        %(artist)s,
        %(collector_number)s,
        %(collector_letter)s,
        %(original_text)s,
        %(original_type)s,
        %(card_id)s,
        ( SELECT id FROM spellbook_set WHERE code = %(setcode)s )
    )""", printing_details )

        cursor.execute("SELECT lastval()")
        rows = cursor.fetchone()
        printing_id = rows[0]

        print('printing_id:{0}'.format(printing_id))

    if card.get('foreignNames') is not None:

        print('card has foreign languages')

        for language in card.get('foreignNames'):
            create_printing_language_for_card(cursor, language['language'], language['name'], printing_id)

    create_printing_language_for_card(cursor, 'English', card['name'], printing_id)

def get_colour_flags_from_names(colour_names):
    flags = 0;
    for colour in colour_names:
        flags |= colour_name_to_flag[colour.lower()]

    return flags

def create_printing_language_for_card(cursor, language, name, printing_id):
    cursor.execute("""
SELECT id
FROM spellbook_cardprintinglanguage
WHERE card_printing_id = %(printing_id)s
AND language = %(language)s
""", { 'language': language, 'name': name, 'printing_id': printing_id } )
    rows = cursor.fetchone()

    language_id = None

    if rows is not None:
        language_id = rows[0]
            
    if language_id is None:

        print('card language does not exist for {0}'.format(language))

        cursor.execute("""
INSERT INTO spellbook_cardprintinglanguage (
    language,
    card_name,
    card_printing_id
) VALUES (
    %(language)s,
    %(card_name)s,
    %(card_printing_id)s
)""", { 'language': language, 'card_name': name, 'card_printing_id': printing_id } )

        print('language_id is now {0}'.format(language_id))

def get_colour_flags_from_codes(colour_codes):
    flags = 0;
    for colour in colour_codes:
        flags |= colour_code_to_flag[colour.lower()]

    return flags

def convert_to_number(val):
    match = re.search( '([\d.]+)', str(val) )
    if match:
        return match.groups(1)

    return 0

if __name__ == "__main__":
    main()
