import json
import psycopg2
import gzip
import io
import requests
import datetime

def get_previous_date_formatted():
    # Get today's date
    today = datetime.date.today()
    # Calculate one day before today
    previous_date = today - datetime.timedelta(days=0)
    # Format the date as YYYYMMDD
    formatted_date = previous_date.strftime("%Y-%m-%d")
    formatted_date_without_slaces = previous_date.strftime("%Y%m%d")
    return formatted_date_without_slaces,formatted_date

def get_us_json_link():
    url = f'https://app.ticketmaster.com/discovery-feed/v2/events'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        us_data = data['countries']['US']
        json_uri = us_data['JSON']['uri']
        return json_uri
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None


def events():
    # formatted_date_without_slaces,current_date = get_previous_date_formatted()
    json_uri = get_us_json_link()
    print(json_uri)
    # Download the gzip file
    response = requests.get(json_uri)
    if response.status_code == 200:
        # Open the gzip file
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as file:
            data = json.load(file)
            for data in data["events"]:
                event_id = data.get("eventId")
                name = data.get("eventName")
                type = data.get("classificationSubGenre")
                locale = data.get("venue").get("venueLocale")
                url = data.get("primaryEventUrl")
                sales = []
                localDate= data.get("eventStartLocalDate")
                localTime= data.get("eventStartLocalTime")
                timezone = data.get("venue").get("venueTimezone")
                dates = {"start": {"dateTBA": False, "dateTBD": False, "timeTBA": False, "dateTime": data.get("eventStartDateTime"),"localDate":localDate,"localTime":localTime},"timezone":timezone}
                dates = json.dumps(dates)
                please_note = data.get("pleaseNote")
                price_range =[]
                promoter = []
                images=data.get("images")
                # Extracting the required fields using list comprehension
                images = [
                    {
                        "url": image["image"]["url"],
                        "ratio": image["image"]["ratio"],
                        "width": image["image"]["width"],
                        "height": image["image"]["height"],
                        "fallback": image["image"]["fallback"]
                    }
                    for image in images
                ]
                # Output the generated list
                images = json.dumps(images)
                info = data.get("eventInfo")
                description = data.get("eventNotes")
                promoters = []
                outlets = None  # You need to extract this from your data
                products = []  # You need to extract this from your data
                seatmap = None  # You need to extract this from your data
                accessibility = []
                ticket_limit = None  # You need to extract this from your data
                external_links = None  # You need to extract this from your data
                aliases = None  # You need to extract this from your data
                localized_aliases = None  # You need to extract this from your data
                genere_name = data.get("classificationGenre")
                segment_name = data.get("classificationSegment")
                # Extracting values for data_to_insert_venue
                venue_id = data.get("venue").get("venueId")
                venue_name = data.get("venue").get("venueName")
                venue_type = None  # You need to extract this from your data
                venue_locale = data.get("venue").get("venueLocale")
                venue_location = []
                venue_timezone = data.get("venue").get("venueTimezone")
                venue_address = []
                venue_city = []
                venue_state = []
                venue_country = []
                venue_postal_code = data.get("venue").get("venueZipCode")
                venue_dmas = []  # You need to extract this from your data
                views=0
                classification_family=None
                try:
                    conn = psycopg2.connect(
                    host="",
                    user="",
                    password="",
                    database="",
                    port=5432,
                    )
                    print(f"Connection successfull : {conn}")
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



if __name__ == "__main__":
    
    # 0 17 * * 1 /usr/bin/python3 /home/ubuntu/ticket-master-apis/hello_world/app.py >> /home/ubuntu/cronjob_output.txt
    resp = events()
    print(resp)
