import json

import psycopg2
import requests


def events():
    classifications = [
        "cricbuzz&cric",
        "Boston Celtics",
        "Milwaukee Bucks",
        "Philadelphia 76ers",
        "New York Knicks",
        "Cleveland Cavaliers",
        "Indiana Pacers",
        "Miami Heat",
        "Orlando Magic",
        "Chicago Bulls",
        "Atlanta Hawks",
        "Brooklyn Nets",
        "Toronto Raptors",
        "Charlotte Hornets",
        "Washington Wizards",
        "Detroit Pistons",
        "Minnesota Timberwolves",
        "Denver Nuggets",
        "Oklahoma City Thunder",
        "Los Angeles Clippers",
        "Sacramento Kings",
        "Phoenix Suns",
        "New Orleans Pelicans",
        "Dallas Mavericks",
        "Los Angeles Lakers",
        "Utah Jazz",
        "Houston Rockets",
        "Golden State Warriors",
        "Memphis Grizzlies",
        "Portland Trail Blazers",
        "San Antonio Spurs",
        "Detriot",
        "Alternative",
        "Ballads/Romantic",
        "Blues",
        "Children's Music",
        "Classical",
        "Country",
        "Dance/Electronic",
        "Folk",
        "Hip-Hop/Rap",
        "Holiday",
        "Jazz",
        "Latin",
        "Medieval/Renaissance",
        "Metal",
        "New Age",
        "Other",
        "Pop",
        "R&B",
        "Reggae",
        "Religious",
        "Rock",
        "World",
        "Aquatics",
        "Athletic Races",
        "Badminton",
        "Bandy",
        "Baseball",
        "Basketball",
        "Biathlon",
        "Body Building",
        "Boxing",
        "Cricket",
        "Curling",
        "Cycling",
        "Equestrian",
        "eSports",
        "Extreme",
        "Field Hockey",
        "Fitness",
        "Floorball",
        "Football",
        "Golf",
        "Gymnastics",
        "Handball",
        "Hockey",
        "Ice Skating",
        "Indoor Soccer",
        "Lacrosse",
        "Martial Arts",
        "Miscellaneous",
        "Motorsports/Racing",
        "Netball",
        "Rodeo",
        "Roller Derby",
        "RollerHockey",
        "Rugby",
        "Ski Jumping",
        "Skiing",
        "Soccer",
        "Softball",
        "Squash",
        "Surfing",
        "Swimming",
        "Table Tennis",
        "Tennis",
        "Toros",
        "Track & Field",
        "Volleyball",
        "Waterpolo",
        "Wrestling",
        "Broadway",
        "Children's Theatre",
        "Circus & Specialty Acts",
        "Classical",
        "Comedy",
        "Cultural",
        "Dance",
        "Espectaculo",
        "Fashion",
        "Fine Art",
        "Magic & Illusion",
        "Miscellaneous",
        "Multimedia",
        "Music",
        "Opera",
        "Performance Art",
        "Puppetry",
        "Spectacular",
        "Theatre",
        "Variety",
        "Children's Music",
        "Children's Theater",
        "Circus/Specialty Acts",
        "Fairs/Festivals",
        "Film/Family",
        "Ice Shows",
        "Latin Children's",
        "Magic/Illusion",
        "Miscellaneous/Family",
        "Puppetry",
        "Rodeo",
    ]

    conn = psycopg2.connect(
        host="",
        user="",
        password="",
        database="",
        port=5432,
    )
    print(f"Connection successfull : {conn}")
    for classification in classifications:
        try:
            print(classification)
            url = f"https://app.ticketmaster.com/discovery/v2/events.json?keyword={classification}&size=200&apikey="
            payload = json.dumps(
                {"password2": "", "password": ""}
            )
            headers = {
                "Accept": "application/json",
                "Authorization": "Bearer ..",
                "Content-Type": "application/json",
            }
            response = requests.request(
                "GET", url, headers=headers, data=payload
            ).json()
            for i in response["_embedded"]["events"]:
                result_dict = i
                try:
                    event_id = result_dict["id"]
                    print(event_id)
                except KeyError:
                    event_id = "Null"
                try:
                    name = result_dict["name"]

                except KeyError:
                    name = "Null"

                try:
                    type = result_dict["type"]

                except KeyError:
                    type = "Null"

                try:
                    locale = result_dict["locale"]

                except KeyError:
                    locale = "Null"

                try:
                    url = result_dict["url"]

                except KeyError:
                    url = "Null"

                try:
                    sales = result_dict["sales"]
                    sales = json.dumps(sales)

                except KeyError:
                    sales = []

                try:
                    dates = result_dict["dates"]
                    dates = json.dumps(dates)

                except KeyError:
                    dates = []

                try:
                    please_note = result_dict["pleaseNote"]

                except KeyError:
                    please_note = "Null"

                try:
                    images = result_dict["images"]
                    images = json.dumps(images)

                except KeyError:
                    images = []

                result_dict = response.get("_embedded", {}).get("events", [{}])[0]

                try:
                    info = result_dict["info"]

                except KeyError:
                    info = "Null"

                try:
                    classification = result_dict["classifications"]
                    classification = json.dumps(classification)

                except KeyError:
                    classification = []

                try:
                    distance = result_dict["distance"]

                except KeyError:
                    distance = 0.0

                try:
                    units = result_dict["units"]

                except KeyError:
                    units = "Null"

                try:
                    description = result_dict["description"]

                except KeyError:
                    description = "Null"

                try:
                    price_range = result_dict["priceRanges"]
                    price_range = json.dumps(price_range)

                except KeyError:
                    price_range = []
                try:
                    promoter = result_dict["promoter"]
                    promoter = json.dumps(promoter)

                except KeyError:
                    promoter = []
                try:
                    promoters = result_dict["promoter"]
                    promoters = json.dumps(promoters)

                except KeyError:
                    promoters = []

                try:
                    outlets = result_dict["outlets"]
                    outlets = json.dumps(outlets)

                except:
                    outlets = []

                try:
                    products = result_dict["products"]
                    products = json.dumps(products)

                except KeyError:
                    products = []

                try:
                    seatmap = result_dict["seatmap"]
                    seatmap = json.dumps(seatmap)

                except KeyError:
                    seatmap = []

                try:
                    accessibility = result_dict["accessibility"]
                    accessibility = json.dumps(accessibility)

                except KeyError:
                    accessibility = None

                try:
                    ticket_limit = result_dict["ticketLimit"]
                    ticket_limit = json.dumps(ticket_limit)

                except KeyError:
                    ticket_limit = []

                try:
                    external_links = result_dict["externalLinks"]
                    external_links = json.dumps(external_links)

                except KeyError:
                    external_links = []

                try:
                    aliases = result_dict["aliases"]
                    aliases = json.dumps(aliases)

                except KeyError:
                    aliases = []

                try:
                    localized_aliases = result_dict["localizedAliases"]
                    localized_aliases = json.dumps(localized_aliases)

                except KeyError:
                    localized_aliases = []

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_id = venue["id"]
                    # venue_id = result_dict["_embedded"]["venues"][0]["id"]
                    # print(venue_id)
                except KeyError:
                    venue_id = "Null"

                try:
                    # Get venue details
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_name = venue["name"]
                    # venue_name = result_dict["_embedded"]["venues"][0]["name"]
                    print(venue_name)
                except KeyError:
                    venue_name = "Null"

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_type = venue["type"]
                    # venue_type = result_dict["_embedded"]["venues"][0]["type"]

                except KeyError:
                    venue_type = "Null"

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_locale = venue["locale"]
                    # venue_locale = result_dict["_embedded"]["venues"][0]["locale"]

                except KeyError:
                    venue_locale = "Null"

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_location = venue["location"]
                    venue_location = json.dumps(venue_location)

                except KeyError:
                    venue_location = []

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_markets = venue["markets"]
                    venue_markets = json.dumps(venue_markets)

                except KeyError:
                    venue_markets = []

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_postal_code = venue["postalCode"]

                except KeyError:
                    venue_postal_code = "Null"

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_country = venue["country"]
                    venue_country = json.dumps(venue_country)

                except KeyError:
                    venue_country = []

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_state = venue["state"]
                    venue_state = json.dumps(venue_state)

                except KeyError:
                    venue_state = []

                try:
                    if "_embedded" in i and "venues" in i["_embedded"]:
                        venues = i["_embedded"]["venues"]
                        for venue in venues:
                            venue_dmas = venue["dmas"]
                    venue_dmas = json.dumps(venue_dmas)

                except KeyError:
                    venue_dmas = []

                try:

                    venue_timezone = result_dict["_embedded"]["venues"][0]["timezone"]

                except KeyError:
                    venue_timezone = "Null"
                try:
                    venue_address = result_dict["_embedded"]["venues"][0]["address"]
                    venue_address = json.dumps(venue_address)

                except KeyError:
                    venue_address = []

                try:
                    venue_city = result_dict["_embedded"]["venues"][0]["city"]
                    venue_city = json.dumps(venue_city)

                except KeyError:
                    venue_city = []
                try:
                    segment_name = result_dict["classifications"][0]["segment"]["name"]

                except KeyError:
                    segment_name = "Null"
                try:
                    genere_name = result_dict["classifications"][0]["genre"]["name"]

                    print(result_dict["classifications"])
                except KeyError:
                    genere_name = "Null"
                try:
                    classification_family = result_dict["classifications"][0]["family"]

                except KeyError:
                    classification_family = False
                views = 0
                try:
                    cursor = conn.cursor()
                    # Check if the event ID exists in the database
                    cursor.execute(
                        "SELECT event_id FROM ticket_master_event WHERE event_id = %s",
                        (event_id,),
                    )
                    existing_event = cursor.fetchone()

                    # If the event ID exists, you can skip the insertion
                    if existing_event:
                        print(
                            f"Event with event ID {event_id} already exists. Skipping insertion."
                        )
                    else:
                        # Proceed with the INSERT operation

                        insert_query = """
                            INSERT INTO ticket_master_event (event_id, name, type, locale, url, sales, dates, please_note, price_range, promoter, images, info, description, promoters, outlets, products, seatmap, accessibility, ticket_limit, external_links, aliases, localized_aliases,genre_name,segment_name,views)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        insert_query_venue = """
                            INSERT INTO ticket_master_venue (
                                venue_id, name, type, locale, location, timezone,
                                address, city, state, country, postal_code, dmas, event_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        # insert_query_venue = """
                        #     INSERT INTO ticket_master_venue (venue_id, name, type, locale, location, url, timezone, address, city, postal_code, dmas, event_id)
                        #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        # """
                        insert_query_classification = """
                            INSERT INTO ticket_master_classification (genre_name, segment_name, family)
                            VALUES (%s, %s, %s);
                        """

                        # Data to be inserted (as a tuple)
                        data_to_insert = (
                            event_id,
                            name,
                            type,
                            locale,
                            url,
                            sales,
                            dates,
                            please_note,
                            price_range,
                            promoter,
                            images,
                            info,
                            # classification,
                            # distance,
                            # units,
                            description,
                            promoters,
                            outlets,
                            products,
                            seatmap,
                            accessibility,
                            ticket_limit,
                            external_links,
                            aliases,
                            localized_aliases,
                            genere_name,
                            segment_name,
                            views,
                        )

                        data_to_insert_venue = (
                            venue_id,
                            venue_name,
                            venue_type,
                            venue_locale,
                            venue_location,
                            venue_timezone,
                            venue_address,
                            venue_city,
                            venue_state,
                            venue_country,
                            venue_postal_code,
                            venue_dmas,
                            event_id,
                        )

                        data_to_insert_classifications = (
                            genere_name,
                            segment_name,
                            classification_family,
                        )
                        # Execute the query

                        try:
                            cursor.execute(insert_query, data_to_insert)
                        except Exception as e:
                            print(f"event data: {e}")
                        try:
                            cursor.execute(insert_query_venue, data_to_insert_venue)
                        except Exception as e:
                            print(f"venue data: {e}")
                        try:
                            cursor.execute(
                                insert_query_classification,
                                data_to_insert_classifications,
                            )
                        except Exception as e:
                            print(f"classification data: {e}")

                        # Commit the transaction
                        conn.commit()
                        print(f"Data pushed for Event ID :{event_id}")

                except Exception as e:
                    print(f"Data not pushed : {e}")
        except:
            pass
    print("Hello world API - HTTP 200")
    return "response"


if __name__ == "__main__":
    # 0 17 * * 1 /usr/bin/python3 /home/ubuntu/ticket-master-apis/hello_world/app.py >> /home/ubuntu/cronjob_output.txt
    resp = events()
    print(resp)
