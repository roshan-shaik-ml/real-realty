import requests
import psycopg2
from psycopg2.extras import execute_values

# Database connection
conn = psycopg2.connect(
    dbname="lead_generation",
    user="admin",
    password="adminpassword",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# GraphQL API details
ZILLOW_URL = "https://www.zillow.com/graphql/"
HEADERS = {
    "origin": "https://www.zillow.com",
    "referer": "https://www.zillow.com/homes/for_sale/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "user-agent": "Chrome/133.0.0.0"
}

# GraphQL request body
QUERY_BODY = {
    "searchQueryState": {
        "pagination": {"currentPage": 1},
        "mapBounds": {
            "west": -119.21510774414062,
            "east": -117.60835725585937,
            "south": 33.56513304568846,
            "north": 34.47457151586186
        },
        "filterState": {
            "sortSelection": {"value": "globalrelevanceex"}
        },
        "isListVisible": True
    },
    "wants": {
        "cat1": ["mapResults", "listResults"],
        "cat2": ["total"]
    }
}

# Send request
response = requests.post(ZILLOW_URL, json=QUERY_BODY, headers=HEADERS)
if response.status_code != 200:
    print("Failed to fetch data from Zillow")
    exit()

data = response.json()
houses_data = data.get("data", {}).get("cat1", {}).get("searchResults", {}).get("listResults", [])

houses = []
addresses = []
images = []

for house in houses_data:
    house_id = house.get("zpid")
    houses.append((
        house_id,
        house.get("price"),
        house.get("unformattedPrice"),
        house.get("statusType"),
        house.get("lotAreaString"),
        house.get("hdpData", {}).get("homeInfo", {}).get("homeType"),
        house.get("latLong", {}).get("latitude"),
        house.get("latLong", {}).get("longitude"),
        house.get("brokerName"),
        house.get("detailUrl")
    ))

    addresses.append((
        house_id,
        house.get("addressStreet"),
        house.get("addressCity"),
        house.get("addressState"),
        house.get("addressZipcode")
    ))

    for img in house.get("carouselPhotos", []):
        images.append((house_id, img.get("url")))

# Insert into PostgreSQL
house_insert_query = """
    INSERT INTO houses (zpid, price, unformatted_price, status, lot_area, home_type, latitude, longitude, broker_name, detail_url)
    VALUES %s
    ON CONFLICT (zpid) DO NOTHING;
"""
execute_values(cursor, house_insert_query, houses)

address_insert_query = """
    INSERT INTO addresses (house_id, street, city, state, zipcode)
    VALUES %s
    ON CONFLICT (house_id) DO NOTHING;
"""
execute_values(cursor, address_insert_query, addresses)

image_insert_query = """
    INSERT INTO house_images (house_id, image_url)
    VALUES %s
    ON CONFLICT DO NOTHING;
"""
execute_values(cursor, image_insert_query, images)

# Commit and close
conn.commit()
cursor.close()
conn.close()
print("Data successfully inserted!")
