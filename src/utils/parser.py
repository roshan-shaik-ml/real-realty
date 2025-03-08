import logging
import uuid
import random
from utils.logger import setup_logger

# Setup logger
logger = setup_logger()


class Parser:
    def __init__(self):
        pass

    def generate_random_zpid(self):

        # 9 digit number as string
        return str(random.randint(100000000, 999999999))

    def parse_house_data(self, house):

        id = uuid.uuid4()

        # Get zpid from the house data or generate a random one

        if house.get("id"):
            zpid = house.get("id")
        elif house.get("zpid"):
            zpid = house.get("zpid")
        else:
            zpid = self.generate_random_zpid()

        # Clean price value
        price = house.get("unformattedPrice")
        if isinstance(price, str):
            try:
                price = float("".join(filter(str.isdigit, price)))
            except (ValueError, TypeError):
                logger.warning(f"Could not parse price value: {price}")
                return None
        elif price is None:
            price = 0

        # Get home_type from hdpData.homeInfo.homeType
        home_type = house.get("hdpData", {}).get("homeInfo", {}).get("homeType")

        # Get area value from hdpData.homeInfo.livingArea
        area = house.get("hdpData", {}).get("homeInfo", {}).get("livingArea", 0)
        if isinstance(area, str):
            try:
                area = float(area)
            except (ValueError, TypeError):
                area = None
                logger.warning(f"Could not parse area value: {area}")

        beds = int(house.get("beds", 0))
        if beds == 0:
            try:
                beds = int(house.get("hdpData", {}).get("homeInfo", {}).get("beds", 0))
            except (ValueError, TypeError):
                logger.warning(f"Could not parse beds value: {beds}")

        baths = int(house.get("baths", 0))
        if baths == 0:
            try:
                baths = int(
                    house.get("hdpData", {}).get("homeInfo", {}).get("baths", 0)
                )
            except (ValueError, TypeError):
                logger.warning(f"Could not parse baths value: {baths}")

        status_type = house.get("statusType", "STATUS_TYPE_UNKNOWN")
        logger.warn(f"baths and beds: {baths} {beds}")
        house_data = [
            id,
            zpid,
            price,
            status_type,
            beds,
            baths,
            area,
            home_type,
            house.get("detailUrl"),
        ]
        return house_data

    def parse_address_data(self, house, house_id):

        # Minimum required fields is zipcode
        zipcode = None
        if house.get("zipcode"):
            zipcode = house.get("zipcode")
        elif house.get("hdpData", {}).get("homeInfo", {}).get("zipcode"):
            zipcode = house.get("hdpData", {}).get("homeInfo", {}).get("zipcode")
        else:
            return None

        return [
            uuid.uuid4(),
            house.get("hdpData", {}).get("homeInfo", {}).get("streetAddress"),
            house.get("hdpData", {}).get("homeInfo", {}).get("city"),
            house.get("hdpData", {}).get("homeInfo", {}).get("state"),
            zipcode,
            house.get("latLong", {}).get("latitude")
            or house.get("hdpData", {}).get("homeInfo", {}).get("latitude"),
            house.get("latLong", {}).get("longitude")
            or house.get("hdpData", {}).get("homeInfo", {}).get("longitude"),
            house_id,
        ]

    def parse_image_data(self, house, house_id):
        """Parse image data from house object."""
        images = []
        carousel_photos = house.get("carouselPhotos", [])
        logger.warn(
            f"Found {len(carousel_photos)} carousel photos for house {house_id}"
        )

        for img in carousel_photos:
            if img.get("url"):
                logger.warn(f"Processing image URL: {img.get('url')}")
                images.append([uuid.uuid4(), house_id, img.get("url")])
            else:
                logger.warning(f"Image object missing URL: {img}")

        logger.warn(f"Successfully parsed {len(images)} images for house {house_id}")
        return images

    def reset_data(self, brokers_data):

        brokers_data.clear()
