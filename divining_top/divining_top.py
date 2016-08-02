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
import mysql.connector

dataFolder = '../data'

parser = OptionParser()

parser.add_option("-c", "--connection", dest="connection_string",
                  help="The postgres connection string")


parser.add_option("-m", "--mysql_connection", dest="mysql_connection_string",
                  help="The mysql connection string")

parser.add_option("-d", "--download", action="store_true", dest="download",
                  help="downloads the json file even if it already exists")

parser.add_option("-i", "--imagedir", dest="image_folder",
                  help="The location to download the card images to")

parser.add_option("-r", "--reset", dest="reset_database",
                  help="Whether to reset the database before loading the data")

(options, args) = parser.parse_args()

json_zip_file = path.join(dataFolder, 'AllSets-x.json.zip')
json_data_file = path.join(dataFolder, 'AllSets-x.json')
pretty_json_file = path.join(dataFolder, 'AllSets-x-pretty.json')
json_pretty_file = path.join(dataFolder, 'AllSets-x-pretty.json')

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

image_download_queue = queue.Queue()
image_download_url = 'http://gatherer.wizards.com/Handlers/Image.ashx?multiverseid={0}&type=card'


def main():

    new_data_file = False

    if options.download or not path.isfile(json_data_file):
        download_json_data()
        new_data_file = True

    json_data = parse_json_data()

    if new_data_file:
        pretty_print_json_data(json_data)

    connection = connect_to_database()

    if options.reset_database:
        reset_database(connection)

    update_rarity_table(connection)

    update_language_information(connection)
    update_block_information(json_data, connection)
    update_set_information(json_data, connection)
    update_card_information(json_data, connection)
    update_ruling_table(json_data, connection)
    update_physical_cards(json_data, connection)

    if options.mysql_connection_string:
        migrate_database(connection)

    connection.commit()

    if options.image_folder:
        download_card_images(connection)

    connection.close()


def parse_json_data():
    f = open(json_data_file, 'r', encoding="utf8")
    json_data = json.load(f, encoding='UTF-8')
    f.close()

    json_data = sorted(json_data.items(),
                       key=lambda set: set[1]["releaseDate"])

    return json_data


def pretty_print_json_data(json_data):
    f = open(pretty_json_file, 'w', encoding='utf8')
    f.write(json.dumps(json_data,
                       sort_keys=True,
                       indent=2,
                       separators=(',', ':')))
    f.close()


def download_json_data():

    url = "http://mtgjson.com/json/AllSets-x.json.zip"
    r = requests.get(url)

    with open(json_zip_file, 'wb') as output:
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
ALTER SEQUENCE spellbook_cardprinting_id_seq RESTART;
ALTER SEQUENCE spellbook_cardprintinglanguage_id_seq RESTART;
ALTER SEQUENCE spellbook_cardruling_id_seq RESTART;
TRUNCATE spellbook_physicalcard CASCADE;
ALTER SEQUENCE spellbook_physicalcard_id_seq RESTART;
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

def update_language_information(connection):
    
    cursor = connection.cursor()

    cursor.execute("""
INSERT INTO spellbook_language (
    name,
    mci_code
) VALUES (
   'English',
   'en'
), (
   'Chinese Simplified',
   'tw'
), (
   'Chinese Traditional',
   'ch'
), (
   'French',
   'fr'
), (
   'German',
   'de'
), (
   'Italian',
   'it'
), (
   'Japanese',
   'jp'
), (
   'Italian',
   'it'
), (
   'Portuguese (Brazil)',
   'pt'
), (
   'Russian',
   'ru'
), (
   'Spanish',
   'es'
), (
   'Korean',
   NULL
)
ON CONFLICT(name) DO NOTHING;
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
) VALUES (
    %(block_name)s,
    %(release_date)s
) ON CONFLICT (name) DO NOTHING
        """, {
        'block_name': set[1]['block'],
        'release_date': set[1]['releaseDate']
        })

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
    block_id,
    mci_code
) VALUES (
    %(set_code)s,
    %(set_name)s,
    %(release_date)s,
    (SELECT id FROM spellbook_block WHERE name = %(block_name)s),
    %(mci_code)s
) ON CONFLICT (code) DO UPDATE SET mci_code = EXCLUDED.mci_code""", {
            'set_code': set[0],
            'set_name': set[1]['name'],
            'release_date': set[1]['releaseDate'],
            'block_name': set[1].get('block'),
            'mci_code': set[1].get('magicCardsInfoCode')
        })

    cursor.close()

    print("Done\n")


def update_card_information(json_data, connection):

    cursor = connection.cursor()

    for set in json_data:

        collector_number = 0
        for card in set[1]['cards']:
            collector_number += 1
            update_card(card, set[0], cursor, collector_number)

    cursor.close()


def get_card_id(cursor, card_name):

    # Find whether a card of the given name exists in the database yet
    cursor.execute("""
SELECT c.id card_id
FROM spellbook_card c
WHERE c.name = %(name)s
    """, {'name': card_name})

    assert(cursor.rowcount < 2)

    rows = cursor.fetchone()
    if rows is not None:
        return rows[0]

    return None

def get_card_printing_id(cursor, card_id, setcode, collector_number, collector_letter):
    
    cursor.execute("""
SELECT id
FROM spellbook_cardprinting
WHERE card_id = %(card_id)s
AND set_id = ( SELECT id FROM spellbook_set WHERE code = %(setcode)s )
AND collector_number = %(collector_number)s
AND collector_letter IS NOT DISTINCT FROM %(collector_letter)s
    """,
    {
        'card_id': card_id,
        'setcode': setcode,
        'collector_number': collector_number,
        'collector_letter': collector_letter
    })

    assert(cursor.rowcount < 2)

    rows = cursor.fetchone()
    if rows is not None:
        return rows[0]
   
    return None

def get_card_printing_language_id(cursor, printing_id, language):

    cursor.execute("""
SELECT id
FROM spellbook_cardprintinglanguage
WHERE card_printing_id = %(printing_id)s
AND language_id = ( SELECT id FROM spellbook_language WHERE name = %(language)s )
""", {
        'language': language,
        'printing_id': printing_id
    })
    rows = cursor.fetchone()

    if rows is None:
        return None

    return rows[0]

def get_card_details(card):
    
    card_colour = 0
    if card.get('colors'):
        card_colour = get_colour_flags_from_names(card['colors'])


    card_details = {
        'name': card['name'],
        'cost': card.get('manaCost'),
        'cmc': card.get('cmc') or 0,
        'colour': card_colour,
        'colour_identity':
            get_colour_flags_from_codes(card['colorIdentity'])
            if card.get('colourIdentity')
            else 0,
        'colour_count': bin(card_colour).count('1'),
        'type':
            ' '.join(card['types'])
            if card.get('types')
            else None,
        'subtype':
            ' '.join(card['subtypes'])
            if card.get('subtypes')
            else None,
        'power': card.get('power'),
        'num_power':
            convert_to_number(card['power'])
            if card.get('power')
            else 0,
        'toughness': card.get('toughness'),
        'num_toughness': convert_to_number(card['toughness'])
            if card.get('toughness')
            else 0,
        'loyalty': card.get('loyalty'),
        'num_loyalty': convert_to_number(card['loyalty'])
            if card.get('loyalty')
            else 0,
        'rules_text': card.get('text'),
    }

    return card_details

def get_card_printing_details(card, setcode, collector_number):

    cnum_match = None
    if card.get('number'):
        re.search('^(?P<special>[a-z]+)?(?P<number>[0-9]+)(?P<letter>[a-z]+)?$',
            card['number'])

    printing_details = {
        'rarity':
            'Timeshifted'
            if card.get('timeshifted') and card['timeshifted']
            else card.get('rarity'),
        'flavour_text': card.get('flavor'),
        'artist': card['artist'],
        'collector_number':
            cnum_match.group('number')
            if cnum_match
            else collector_number,
        'collector_letter':
            cnum_match.group('special') or cnum_match.group('letter')
            if cnum_match
            else None,
        'original_text': card.get('originalText'),
        'original_type': card.get('originalType'),
        'setcode': setcode
    }

    return printing_details


def update_card(card, setcode, cursor, collector_number):

    print('Updating card {0}'.format(card['name']).encode())

    card_details = get_card_details(card)

    printing_details = get_card_printing_details(card, setcode, collector_number)

    language_details = {
        'language': 'English',
        'card_name': card['name'],
    }

    card_id = get_card_id(cursor, card['name'])

    # If the card does not exist in the database, then the
    if card_id is None:

        cursor.execute("""
INSERT INTO spellbook_card (
    name,
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
    %(name)s,
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
        (card_id) = rows[0]

        printing_details['card_id'] = card_id

        print('Inserted new card record {0}'.format(card_id))

    else:  # card_id is not None

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
    rules_text = %(rules_text)s
WHERE id = %(card_id)s
        """, card_details)

    printing_id = get_card_printing_id(cursor, card_id, setcode, printing_details['collector_number'], printing_details['collector_letter'])

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


    language_id = get_or_create_card_language(cursor, 'English', card['name'], printing_id, card.get('multiverseid'))

    if card.get('foreignNames'):

        for language in card.get('foreignNames'):
            language_id = get_or_create_card_language(cursor,
                                               language['language'],
                                               language['name'],
                                               printing_id,
                                               language.get('multiverseid'))


def get_colour_flags_from_names(colour_names):
    flags = 0
    for colour in colour_names:
        flags |= colour_name_to_flag[colour.lower()]

    return flags


def get_or_create_card_language(cursor, language, name, printing_id, multiverse_id):
       
    language_id = get_card_printing_language_id(cursor, printing_id, language)
    if language_id is not None:
        return language_id

    cursor.execute("""
INSERT INTO spellbook_cardprintinglanguage (
    language_id,
    card_name,
    card_printing_id,
    multiverse_id
) VALUES (
    ( SELECT id FROM spellbook_language WHERE name = %(language)s ),
    %(card_name)s,
    %(card_printing_id)s,
    %(multiverse_id)s
)""", {
        'language': language,
        'card_name': name,
        'card_printing_id': printing_id,
        'multiverse_id': multiverse_id
    })

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
        SELECT id
        FROM spellbook_card
        WHERE name = %(card_name)s
    )
) ON CONFLICT (date, text, card_id) DO NOTHING
                """, {
                    'card_name': card['name'],
                    'ruling_date': ruling['date'],
                    'ruling_text': ruling['text']
                })

    cursor.close()


def update_physical_cards(json_data, connection):

    cursor = connection.cursor()

    for set in json_data:

        setcode = set[0]

        collector_number = 0
        for card_data in set[1]['cards']:
            collector_number += 1
            
            card_id = get_card_id(cursor, card_data['name'])
            assert(card_id is not None)
            printing_details = get_card_printing_details(card_data, setcode, collector_number)
            printing_id = get_card_printing_id(cursor, card_id, setcode, printing_details['collector_number'], printing_details['collector_letter'])
            assert(printing_id is not None)

            language_id = get_card_printing_language_id(cursor, printing_id, 'English')
            update_physical_card_info(cursor, card_data, language_id)

            if card_data.get('foreignNames'):
                for card_language in card_data.get('foreignNames'):
                    
                    language_id = get_card_printing_language_id(cursor, printing_id, card_language['language'])

                    update_physical_card_info(cursor, card_data, language_id)




def update_physical_card_info(cursor, card_data, language_id):

    # Don't do anything for the back half of meld cards (their children/front cards will set up the physical ID)
    if card_data['layout'] == 'meld' and len(card_data['names']) == 3:
        return

    cursor.execute("""
SELECT physical_card_id
FROM spellbook_physicalcardlink
WHERE printing_language_id = %(language_id)s
    """, { 'language_id': language_id } )

    row = cursor.fetchone()
    physical_id = row[0] if row is not None else None

    if physical_id is not None:
        print('Physical ID for {0} already exists'.format(card_data['name']))
        # Physical card and link already exists, no work to be done
        return

    cursor.execute("""
SELECT
  lang.name,
  printing.set_id,
  collector_number,
  collector_letter
FROM spellbook_cardprintinglanguage printlang
JOIN spellbook_cardprinting printing
  ON printing.id = printlang.card_printing_id
JOIN spellbook_language lang
  ON lang.id = printlang.language_id
WHERE printlang.id = %(language_id)s
    """, { 'language_id': language_id } )
    row = cursor.fetchone()
    assert(row is not None)
    (language, set_id, collector_number, collector_letter) = row

    linked_language_ids = []

    if card_data.get('names'):

        for link_name in card_data['names']:

            if link_name == card_data['name']:
                continue

            cursor.execute("""
SELECT printlang.id
FROM spellbook_card card
JOIN spellbook_cardprinting printing
  ON printing.card_id = card.id
 AND printing.set_id = %(set_id)s
JOIN spellbook_cardprintinglanguage printlang
  ON printlang.card_printing_id = printing.id
JOIN spellbook_language lang
  ON lang.id = printlang.language_id
  AND lang.name = %(language)s
WHERe card.name = %(linked_name)s
        """, { 'set_id': set_id, 'language': language, 'linked_name': link_name } )

            row = cursor.fetchone()
            if row is None:
                print('{0} has no physical ID'.format(link_name))
                continue

            assert(cursor.rowcount == 1)

            link_language_id = row[0]

            linked_language_ids.append( link_language_id )

    cursor.execute("""
INSERT INTO spellbook_physicalcard (
    layout
) VALUES (
    %(layout)s
) RETURNING id
    """, { 'layout':  'meld-back' if card_data['layout'] == 'meld' and len(card_data['names']) == 3 else card_data['layout']} ) 
    rows = cursor.fetchone()
    physical_id = rows[0]

    linked_language_ids.append(language_id)

    for id in linked_language_ids:
        print('linking to {0}'.format(id))
        cursor.execute("""
INSERT INTO spellbook_physicalcardlink (
    physical_card_id,
    printing_language_id
) VALUES (
    %(physical_id)s,
    %(language_id)s
) ON CONFLICT (physical_card_id, printing_language_id) DO NOTHING""",  { 'language_id': id, 'physical_id': physical_id } )
        print('Creating new card link of {0}'.format(id))


def download_image_for_card(multiverse_id):

    image_path = options.image_folder + str(multiverse_id) + '.jpg'

    print('Downloading {0}'.format(multiverse_id))

    stream = requests.get(image_download_url.format(multiverse_id))

    with open(image_path, 'wb') as output:
        output.write(stream.content)


def download_card_images(connection):

    cursor = connection.cursor()

    cursor.execute("""
SELECT DISTINCT cpl.multiverse_id
FROM spellbook_cardprintinglanguage cpl
WHERE cpl.multiverse_id IS NOT NULL
ORDER BY cpl.multiverse_id
    """)

    data = cursor.fetchall()

    for row in data:
        multiverse_id = row[0]
        image_path = options.image_folder + str(multiverse_id) + '.jpg'

        # Skip the image if it already exists
        if path.exists(image_path):
            continue

        image_download_queue.put(multiverse_id)

    cursor.close()

    for i in range(1, 8):
        thread = imageDownloadThread(image_download_queue)
        thread.setDaemon(True)
        thread.start()

    image_download_queue.join()


def migrate_database(connection):

    connectParams = dict(entry.split('=') for entry in options.mysql_connection_string.split(';'))
    cnx = mysql.connector.connect(**connectParams)

    mysql_cursor = cnx.cursor()
    postgres_cursor = connection.cursor()

    postgres_cursor.execute("""
TRUNCATE spellbook_userownedcard CASCADE;
ALTER SEQUENCE spellbook_userownedcard_id_seq RESTART;

TRUNCATE spellbook_usercardchange CASCADE;
ALTER SEQUENCE spellbook_usercardchange_id_seq RESTART;
    """)

    query = """
SELECT c.name, uc.count, uc.setcode
FROM usercards uc
JOIN cards c
ON c.id = uc.cardid
WHERE ownerid = 1
ORDER BY uc.id ASC
    """

    mysql_cursor.execute(query)

    for (card_name, card_count, set_code) in mysql_cursor:

        postgres_cursor.execute("""
SELECT MIN(link.physical_card_id)
FROM spellbook_physicalcardlink link
JOIN spellbook_cardprintinglanguage printlang
  ON printlang.id = link.printing_language_id
JOIN spellbook_cardprinting print
  ON print.id = printlang.card_printing_id
JOIN spellbook_set set
  ON set.id = print.set_id
JOIN spellbook_language lang
  ON lang.id = printlang.language_id
WHERE lang.name = 'English'
AND printlang.card_name = %(card_name)s
AND set.code = %(set_code)s
        """, {'card_name': card_name, 'set_code': set_code } )
        row = postgres_cursor.fetchone()
        physical_id = row[0]

        assert(physical_id is not None)

        postgres_cursor.execute("""
SELECT 1 FROM spellbook_userownedcard
WHERE owner_id = ( SELECT id FROM auth_user WHERE username = 'Liam' )
AND physical_card_id = %(physical_id)s
        """, { 'physical_id': physical_id } )
        row = postgres_cursor.fetchone()
        if row is not None:
            continue

        postgres_cursor.execute("""
INSERT INTO spellbook_userownedcard (
    count,
    physical_card_id,
    owner_id
) VALUES (
    %(card_count)s,
    %(physical_id)s,
    ( SELECT id FROM auth_user WHERE username = 'Liam' )
)
    """, {
            'card_count': card_count,
            'physical_id': physical_id
        })

    query = """
SELECT 
    c.name, 
    ucc.setcode, 
    ucc.datemodified, 
    ucc.difference 
FROM usercardchanges ucc
JOIN cards c
ON c.id = ucc.cardid
WHERE userid = 1
ORDER BY ucc.id ASC
    """

    mysql_cursor.execute(query)

    for (card_name, set_code, date_modified, card_difference) in mysql_cursor:

        postgres_cursor.execute("""
INSERT INTO spellbook_usercardchange (
    difference,
    physical_card_id,
    owner_id,
    date
) VALUES (
    %(card_difference)s,
    (
        SELECT MIN(link.physical_card_id)
        FROM spellbook_physicalcardlink link
        JOIN spellbook_cardprintinglanguage printlang
          ON printlang.id = link.printing_language_id
        JOIN spellbook_cardprinting print
          ON print.id = printlang.card_printing_id
        JOIN spellbook_set set
          ON set.id = print.set_id
        JOIN spellbook_language lang
          ON lang.id = printlang.language_id
        WHERE lang.name = 'English'
        AND printlang.card_name = %(card_name)s
        AND set.code = %(set_code)s
    ),
    ( SELECT id FROM auth_user WHERE username = 'Liam' ),
    %(date_modified)s
)
    """, {
            'card_difference': card_difference,
            'card_name': card_name,
            'set_code': set_code,
            'date_modified': date_modified
        })

    postgres_cursor.close()
    cnx.close()

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
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            multiverse_id = self.queue.get()
            download_image_for_card(multiverse_id)
            self.queue.task_done()


if __name__ == "__main__":
    main()
