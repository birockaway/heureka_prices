import asyncio
import aiohttp
import time
import pandas as pd
import logging
from keboola import docker
import datetime
import os





KBC_DATADIR = os.environ.get("KBC_DATADIR")


utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


logging.basicConfig(format='%(name)s, %(asctime)s, %(levelname)s, %(message)s',
                    level=logging.DEBUG)


cfg = docker.Config(KBC_DATADIR)
parameters = cfg.get_parameters()

logging.info("Extracting parameters from config.")
api_url = parameters.get("api_url")
api_key = parameters.get("#api_key")
language = parameters.get("language")
input_filename = parameters.get("input_filename")
product_id_column_name = parameters.get("product_id_column_name")
max_requests_per_second = parameters.get("max_requests_per_second")
max_requests_bucket_size = parameters.get("max_requests_bucket_size")
wanted_columns = parameters.get("wanted_columns")

# log parameters (excluding sensitive designated by '#')
logging.info({k:v for k,v in parameters.items() if "#" not in k})

input_file = pd.read_csv(KBC_DATADIR + "in/tables/" +
                         input_filename + ".csv", dtype={product_id_column_name:int})


product_list = input_file[product_id_column_name].unique().tolist()

logging.info(f"Input unique products: {len(product_list)}")


START = time.monotonic()

result_list = []

class RateLimiter:
    '''
    limits calls per second
    '''
    RATE = max_requests_per_second  # requests per second
    MAX_TOKENS = max_requests_bucket_size

    def __init__(self, client):
        self.client = client
        self.tokens = self.MAX_TOKENS
        self.updated_at = time.monotonic()

    async def post(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.post(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.RATE
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.MAX_TOKENS)
            self.updated_at = now


def process_product(product_json):
    '''
    extracts product level data from json response
    '''

    product = {}

    product["product_id"] = product_json["id"]
    product["product_name"] = product_json["name"]
    product["product_slug"] = product_json["slug"]
    product["product_min_price"] = product_json["min_price"]
    product["product_url"] = product_json["url"]
    product["product_status"] = product_json["status"]
    product["product_rating"] = product_json["rating"]
    product["product_category"] = product_json["category"]
    product["product_producer"] = product_json["producer"]
    product["product_top_shop"] = product_json["top_shop"]
    product["product_category_position"] = product_json["category_position"]
    product["product_offer_attributes"] = product_json["offer_attributes"]
    product["product_images"] = product_json["images"]

    return product


def process_offer(offer):
    '''
    extracts offer details for each shop from json response
    '''
    offer_items = {"offer_" + k: v for k, v in offer.items()}
    return offer_items

def process_shop(shop, index, index_name="position"):
    '''
    extracts shop level data from json response
    '''
    shop_items = {"shop_" + k: v for k, v in shop.items() if k != "offers"}
    shop_items[index_name] = index + 1
    shop_with_offers = [
        {**shop_items, **process_offer(offer)} for offer in shop["offers"]
    ]
    return shop_with_offers


def process_response(response_json):
    '''
    combines processing of product, shop and offer level data
    '''

    try:
        response_content = response_json["result"]["product"]

    except Exception as e:
        logging.debug('Response does not contain product data.')
        logging.debug(f"Exception {e}")
        logging.debug(response_json)
        return None

    if response_content is None:
        return None

    else:
        try:
            product = process_product(response_content)

            shops_with_positions = [
                process_shop(shop, position, "position")
                for position, shop in enumerate(response_content["shops"])
            ]

            shops_with_positions_flat = [
                item for sublist in shops_with_positions for item in sublist
            ]

            highlighted_shops_with_positions = [
                process_shop(shop, position, "highlighted_position")
                for position, shop in enumerate(response_content["highlighted_shops"])
            ]

            highlighted_shops_with_positions_flat = [
                item for sublist in highlighted_shops_with_positions for item in sublist
            ]

            all_shops = shops_with_positions_flat + highlighted_shops_with_positions_flat
            result = [{**product, **shop} for shop in all_shops]
            return result

        except Exception as e:
            logging.warning(f"Exception {e}")


async def fetch_one(client, url, key, product_id, language):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "product.get",
        "params": {"language": language, "access_key": key, "id": product_id},
    }

    async with await client.post(url, json=payload) as resp:
        resp = await resp.json()
        # parse
        result_list.append(process_response(resp))


async def main():
    async with aiohttp.ClientSession() as client:
        client = RateLimiter(client)
        tasks = [
            asyncio.ensure_future(fetch_one(client, api_url, api_key, product_id, language))
            for product_id in product_list
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

# drop NAs
result_list = (item for item in result_list if item)

# flatten results
results = [item for sublist in result_list for item in sublist]

# get duration
END = time.monotonic()

logging.info(f"Duration: {round(END-START,4)} seconds")
logging.info(f"Output rowcount: {len(results)}")

results_dataframe = pd.DataFrame(results).reindex(wanted_columns, axis='columns')
results_dataframe["utctime_started"] = utctime_started

result_products = results_dataframe["product_id"].unique().tolist()
logging.info(f"Output unique products: {len(result_products)}")

missing_products = list(set(product_list ) - set(result_products))
# log what was not returned
logging.info(f"Missing products: {missing_products}")
missing_products_dataframe = pd.DataFrame({"product_id":missing_products})

logging.info("Writing results.")
results_dataframe.to_csv(KBC_DATADIR + "out/tables/heureka_prices.csv", index=False)
missing_products_dataframe.to_csv(KBC_DATADIR + "out/tables/heureka_missing_products.csv", index=False)

logging.info("Script done.")

