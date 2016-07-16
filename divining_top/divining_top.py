from optparse import OptionParser
import urllib.request
import psycopg2
import zipfile
import json
import requests
import re
import queue
import threading
from os import path
import time

dataFolder = '../data'
image_folder = 'C:/Users/Liam/Projects/sylvan_library/sylvan_library/spellbook/static/card_images/'

parser = OptionParser()

parser.add_option("-c", "--connection", dest="connection_string",
                  help="The postgres connection string")

parser.add_option("-d", "--download", action="store_true", dest="download",
                  help="downloads the json file even if it already exists")

(options, args) = parser.parse_args()

json_zip_file = path.join(dataFolder, 'AllSets-x.json.zip')
json_data_file = path.join(dataFolder, 'AllSets-x.json')
pretty_json_file = path.join(dataFolder, 'AllSets-x-pretty.json')
jsonPrettyFile = path.join(dataFolder, 'AllSets-x-pretty.json')

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

imageDownloadQueueLock = threading.Lock()
imageDownloadQueue = queue.Queue()
imageDownloadThreads = []
imageDownloadExitFlag = False

def main():

    new_data_file = False

    if options.download or not path.isfile(json_data_file):
        download_json_data()
        new_data_file = True

    json_data = parse_json_data()
    
    if new_data_file:
        pretty_print_json_data(json_data)
    
    connection = connect_to_database()

    #reset_database(connection)
    #update_rarity_table(connection)

    update_block_information(json_data, connection)
    update_set_information(json_data, connection)
    update_card_information(json_data, connection)
    update_card_link_information(json_data, connection)
    update_ruling_table(json_data, connection)

    connection.commit()

    download_card_images(connection)

    connection.close()

def parse_json_data():
    f = open(json_data_file, 'r', encoding="utf8")
    json_data = json.load(f, encoding='UTF-8')
    f.close()

    json_data = sorted(json_data.items(), key=lambda set: set[1]["releaseDate"])

    return json_data

def pretty_print_json_data(json_data):
    f = open(pretty_json_file, 'w', encoding='utf8')
    f.write(json.dumps(json_data, sort_keys=True, indent=2, separators=(',', ':')))
    f.close()

def download_json_data():
      
    url = "http://mtgjson.com/json/AllSets-x.json.zip"
    r = requests.get(url)

    with open(json_zip_file,'wb') as output:
        output.write(r.content)
    
    zip = zipfile.ZipFile(json_zip_file)
    zip.extractall(dataFolder)

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
    
    print("Updating block information... ")

    cursor = connection.cursor()

    for set in json_data:

        # Ignore sets that have no block
        if 'block' not in set[1]:
            continue

        cursor.execute("""
INSERT INTO spellbook_block (
    name,
    release_date
) VALUES 
    %(block_name)s, 
    %(release_date)s
) ON CONFLICT (name) 
UPDATE SET release_date = MIN(release_date, EXCLUDED.release_date)
""", { 'block_name': set[1]['block'], 'release_date': set[1]['releaseDate'] })

    cursor.close()

    print("Done\n")

def update_set_information(json_data, connection):
    
    print("Updating set information... ")
    cursor = connection.cursor()

    for set in json_data:
        
        cursor.execute("""
INSERT INTO spellbook_set (
    code,
    name,
    release_date,
    block_id
) VALUES (
    %(set_code)s,
    %(set_name)s,
    %(release_date)s,
    (SELECT id FROM spellbook_block WHERE name = %(block_name)s)
) ON CONFLICT (code) DO NOTHING""",
        { 'set_code': set[0], 'set_name': set[1]['name'], 'release_date': set[1]['releaseDate'], 'block_name': set[1].get('block') })

    cursor.close()

    print("Done\n")

def update_card_information(json_data, connection):

    print("Updating card information... ", end="")
    cursor = connection.cursor()

    for set in json_data:

        #if set[0] != 'ARC':
        #    continue

        collector_number = 0
        for card in set[1]['cards']:
            collector_number += 1
            update_card(card, set[0], cursor, collector_number)

    cursor.close()
    print("Done.")

def update_card(card, setcode, cursor, collector_number):

    print('Updating card {0}'.format(card['name']))

    card_colour = get_colour_flags_from_names(card['colors']) if card.get('colors') else 0

    card_details = {
        'cost': card.get('manaCost'),
        'cmc': card.get('cmc') or 0,
        'colour': card_colour,
        'colour_identity': get_colour_flags_from_codes(card['colorIdentity']) if card.get('colourIdentity') else 0,
        'colour_count': bin(card_colour).count('1'),
        'type': ' '.join(card.get('types')) if card.get('types') else None,
        'subtype': ' '.join(card.get('subtypes')) if card.get('subtypes') else None,
        'power': card.get('power'),
        'num_power': convert_to_number(card.get('power')) if card.get('power') else 0,
        'toughness': card.get('toughness'),
        'num_toughness': convert_to_number(card.get('toughness')) if card.get('toughness') else 0,
        'loyalty': card.get('loyalty'),
        'num_loyalty': convert_to_number(card.get('loyalty')) if card.get('loyalty') else 0,
        'rules_text': card.get('text'),
        'layout': card.get('layout') or 'normal'
    }

    cnum_match = re.search('^(?P<special>[a-z]+)?(?P<number>[0-9]+)(?P<letter>[a-z]+)?$', card['number']) if card.get('number') else None

    printing_details = {
        'rarity': 'Timeshifted' if card.get('timeshifted') and card['timeshifted'] else card.get('rarity'),
        'flavour_text': card.get('flavor'),
        'artist': card['artist'],
        'collector_number': cnum_match.group('number') if cnum_match else collector_number,
        'collector_letter': cnum_match.group('special') or cnum_match.group('letter') if cnum_match else None,
        'original_text': card.get('originalText'),
        'original_type': card.get('originalType'),
        'setcode': setcode
    }

    #if cnum_match.group('letter'):
    #    print('hello')
        
    #if printing_details['collector_number'] == 10:
    #    print('argh')

    language_details = {
        'language': 'English',
        'card_name': card['name'],
    }

    # Find whether a card of the given name exists in the database yet
    cursor.execute("""
SELECT DISTINCT 
  c.id card_id
FROM spellbook_card c
JOIN spellbook_cardprinting cp
  ON cp.card_id = c.id
JOIN spellbook_cardprintinglanguage cpl
  ON cpl.card_printing_id = cp.id
WHERE cpl.language = 'English'
AND cpl.card_name = %(name)s
    """, {'name': card['name']})

    assert(cursor.rowcount < 2)

    rows = cursor.fetchone()
    card_id = None
    if rows is not None:
        card_id = rows[0]

    # If the card does not exist in the database, then the
    if card_id is None:

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
    rules_text,
    layout
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
    %(rules_text)s,
    %(layout)s
)
        """, card_details)

        cursor.execute("SELECT lastval()")
        rows = cursor.fetchone()
        (card_id) = rows[0]
        
        printing_details['card_id'] = card_id

        print('Inserted new card record {0}'.format(card_id))

    else: # card_id is not None

        print('Updating card record {0}'.format(card_id))

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
    rules_text = %(rules_text)s,
    layout = %(layout)s
WHERE id = %(card_id)s
        """, card_details)

    
    cursor.execute("""
SELECT id
FROM spellbook_cardprinting
WHERE card_id = %(card_id)s
AND set_id = ( SELECT id FROM spellbook_set WHERE code = %(setcode)s )
AND collector_number = %(collector_number)s
AND collector_letter IS NOT DISTINCT FROM %(collector_letter)s
    """, { 'card_id': card_id, 'setcode': setcode, 'collector_number': printing_details['collector_number'], 'collector_letter': printing_details['collector_letter'] })
    
    assert(cursor.rowcount < 2)

    rows = cursor.fetchone()
    if rows is not None:
        printing_id = rows[0]
        print('card printing already exists {0}'.format(printing_id))
    else:
        printing_id = None

    if printing_id is None:

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
)
        """, printing_details)

        cursor.execute("SELECT lastval()")
        rows = cursor.fetchone()
        printing_id = rows[0]

        print('Inserted new card printing {0}'.format(printing_id))
    else:
        printing_details['printing_id'] = printing_id

        cursor.execute("""
UPDATE spellbook_cardprinting SET
rarity_id = (SELECT id FROM spellbook_rarity WHERE name = %(rarity)s ),
flavour_text = %(flavour_text)s,
artist = %(artist)s,
collector_number = %(collector_number)s,
collector_letter = %(collector_letter)s,
original_text = %(original_text)s,
original_type = %(original_type)s,
card_id = %(card_id)s
WHERE id = %(printing_id)s
""", printing_details)

    if card.get('foreignNames'):

        for language in card.get('foreignNames'):
            language_id = create_printing_language_for_card(cursor, language['language'], language['name'], printing_id, language.get('multiverseid'))

            #if language.get('multiverseid'):
            #    download_image_for_card(language_id, language['multiverseid'])


    language_id = create_printing_language_for_card(cursor, 'English', card['name'], printing_id, card.get('multiverseid'))

   # if card.get('multiverseid'):
   #     download_image_for_card(language_id, card['multiverseid'])
def get_colour_flags_from_names(colour_names):
    flags = 0
    for colour in colour_names:
        flags |= colour_name_to_flag[colour.lower()]

    return flags

def create_printing_language_for_card(cursor, language, name, printing_id, multiverse_id):
    
    cursor.execute("""
SELECT id
FROM spellbook_cardprintinglanguage
WHERE card_printing_id = %(printing_id)s
AND language = %(language)s
""", { 'language': language, 'name': name, 'printing_id': printing_id })
    rows = cursor.fetchone()

    language_id = None

    if rows is not None:
        language_id = rows[0]

        print('Language already exists for {0} {1}'.format(language, language_id))
        return language_id    


    cursor.execute("""
INSERT INTO spellbook_cardprintinglanguage (
    language,
    card_name,
    card_printing_id,
    multiverse_id
) VALUES (
    %(language)s,
    %(card_name)s,
    %(card_printing_id)s,
    %(multiverse_id)s
)""", { 'language': language, 'card_name': name, 'card_printing_id': printing_id, 'multiverse_id': multiverse_id })

    cursor.execute("SELECT lastval()")
    rows = cursor.fetchone()
    language_id = rows[0]

    print('Inserted card language for {0} {1}'.format(language, language_id))

    return language_id

def update_ruling_table(json_data, connection):

    # The rulings table can be safely truncated and rebuilt because there are
    # no other tables that reference it
    cursor = connection.cursor()

    cursor.execute("""
TRUNCATE spellbook_cardruling;
ALTER SEQUENCE spellbook_cardruling_id_seq RESTART;
""")

    for set in json_data:

        for card in set[1]['cards']:
            
            # Skip cards that don't have additional names (links to other
            # cards)
            if not card.get('rulings'):
                continue

            for ruling in card['rulings']:

                cursor.execute("""
INSERT INTO spellbook_cardruling (
    date,
    text,
    card_id
) VALUES (
    %(ruling_date)s,
    %(ruling_text)s,
    (
        SELECT DISTINCT cp.card_id
        FROM spellbook_cardprinting cp
        JOIN spellbook_cardprintinglanguage cpl
        ON cpl.card_printing_id = cp.id
        WHERE cpl.card_name = %(card_name)s
        AND cpl.language = 'English'
    )
) ON CONFLICT (date, text, card_id) DO NOTHING
                """, {'card_name': card['name'], 'ruling_date': ruling['date'], 'ruling_text': ruling['text'] })

    cursor.close()

    
def update_card_link_information(json_data, connection):

    cursor = connection.cursor()

     # The card link table can be safely truncated and rebuilt because there
     # are no other tables that reference it
    cursor.execute("""
TRUNCATE spellbook_cardlink;
ALTER SEQUENCE spellbook_cardlink_id_seq RESTART;
""")

    for set in json_data:

        for card in set[1]['cards']:
            
            # Skip cards that don't have additional names (links to other
            # cards)
            if not card.get('names'):
                continue

            for link_name in card['names']:

                # Skip the name of the card itself
                if link_name == card['name']:
                    continue
                
                cursor.execute("""
INSERT INTO spellbook_cardlink (
    card_from_id,
    card_to_id
) VALUES (
    (
        SELECT DISTINCT cp.card_id
        FROM spellbook_cardprinting cp
        JOIN spellbook_cardprintinglanguage cpl
        ON cpl.card_printing_id = cp.id
        AND cpl.language = 'English' 
        AND cpl.card_name = %(card_from_name)s
    ),
    (
        SELECT DISTINCT cp.card_id
        FROM spellbook_cardprinting cp
        JOIN spellbook_cardprintinglanguage cpl
        ON cpl.card_printing_id = cp.id
        AND cpl.language = 'English' 
        AND cpl.card_name = %(card_to_name)s
    )
) ON CONFLICT (card_from_id, card_to_id) DO NOTHING
                """, {'card_from_name': card['name'], 'card_to_name': link_name })

    cursor.close()


def download_image_for_card(multiverse_id):
    
    image_path = image_folder + str(multiverse_id) + '.jpg'

    print('Downloading {0}'.format(multiverse_id))

    base_url = "http://gatherer.wizards.com/Handlers/Image.ashx?multiverseid={0}&type=card"
    stream = requests.get(base_url.format(multiverse_id))

    with open(image_path, 'wb') as output:
        output.write(stream.content)

def download_card_images(connection):

    imageDownloadExitFlag = False

    cursor = connection.cursor()

    cursor.execute("""
SELECT DISTINCT cpl.multiverse_id
FROM spellbook_cardprintinglanguage cpl
WHERE cpl.multiverse_id IS NOT NULL
ORDER BY cpl.multiverse_id
    """)

    data = cursor.fetchall()

    imageDownloadQueueLock.acquire()

    for row in data:
        multiverse_id = row[0]
        image_path = image_folder + str(multiverse_id) + '.jpg'

        if path.exists(image_path):
            print('Skipping {0}'.format(multiverse_id))
            continue

        imageDownloadQueue.put(multiverse_id)

    cursor.close()
    imageDownloadQueueLock.release()
    
    for i in range(1,8):
        thread = imageDownloadThread(i)
        thread.start()
        imageDownloadThreads.append(thread)

    while not imageDownloadQueue.empty():
        pass
    
    imageDownloadExitFlag = True   

    for t in imageDownloadThreads:
        t.join()

def get_colour_flags_from_codes(colour_codes):
    flags = 0
    for colour in colour_codes:
        flags |= colour_code_to_flag[colour.lower()]

    return flags

def convert_to_number(val):
    match = re.search('([\d.]+)', str(val))
    if match:
        return match.groups(1)

    return 0

class imageDownloadThread(threading.Thread):
    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID

    def run(self):
        while not imageDownloadExitFlag:
            imageDownloadQueueLock.acquire()
            if not imageDownloadQueue.empty():
                multiverse_id = imageDownloadQueue.get()
                imageDownloadQueueLock.release()
                download_image_for_card(multiverse_id)
            else:
                imageDownloadQueueLock.release()

            time.sleep(1)

if __name__ == "__main__":
    main()
