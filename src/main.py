import asyncio
import aiohttp
import time
import logging
from keboola import docker
import datetime
from collections import defaultdict
import csv
import os
from contextlib import suppress, contextmanager


def process_product(product_json):
    """
    extracts product level data from json response
    """

    return {
        f'product_{k}': v
        for k, v
        in product_json.items()
        if k in {
            "id",
            "name",
            "slug",
            "min_price",
            "url",
            "status",
            "rating",
            "category",
            "producer",
            "top_shop",
            "category_position",
            "offer_attributes",
            "images",
        }
    }


def process_offer(offer):
    """
    extracts offer details for each shop from json response
    """
    offer_items = {"offer_" + k: v for k, v in offer.items()}
    return offer_items


def process_shop(shop, index, index_name):
    """
    extracts shop level data from json response
    """
    shop_items = {"shop_" + k: v for k, v in shop.items() if k != "offers"}
    shop_items[index_name] = index
    shop_with_offers = [
        {**shop_items, **process_offer(offer)} for offer in shop["offers"]
    ]
    return shop_with_offers


def process_response(response_json):
    """
    combines processing of product, shop and offer level data
    """

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
                for position, shop in enumerate(response_content["shops"], start=1)
            ]

            shops_with_positions_flat = [
                item for sublist in shops_with_positions for item in sublist
            ]

            highlighted_shops_with_positions = [
                process_shop(shop, position, "highlighted_position")
                for position, shop in enumerate(response_content["highlighted_shops"], start=1)
            ]

            highlighted_shops_with_positions_flat = [
                item for sublist in highlighted_shops_with_positions for item in sublist
            ]

            all_shops = shops_with_positions_flat + highlighted_shops_with_positions_flat
            result = [{**product, **shop} for shop in all_shops]
            return result

        except Exception as e:
            logging.exception(e)

            return None


def batches(product_list, batch_size, window_size, sleep_time=5):
    window_start = time.monotonic()
    while product_list:
        batch = product_list[:batch_size]
        del product_list[:batch_size]

        while time.monotonic() - window_start < window_size:
            logging.info('waiting for time window to expire...')
            time.sleep(sleep_time)
        window_start = time.monotonic()
        yield batch


class PriceWriter:

    def __init__(self, target_file_name, colnames, prod_id_colname):
        self.result_file = None
        self.result_file_name = target_file_name
        self.writer = None
        self.colnames = colnames

        self.prod_id_colname = prod_id_colname
        self.result_products = set()
        self.total_rows = 0

    def __enter__(self):
        with suppress(FileNotFoundError):
            os.remove(self.result_file_name)

        self.result_file = open(self.result_file_name, 'a', encoding='utf8')
        self.writer = csv.DictWriter(self.result_file, fieldnames=self.colnames)
        self.writer.writeheader()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with suppress(FileNotFoundError):
            self.result_file.close()

    def writerows(self, rows):
        self.writer.writerows(rows)
        self.result_products.update(row.get(self.prod_id_colname) for row in rows if row)
        self.total_rows += len(rows) if rows else 0


@contextmanager
def time_logger():
    start = time.monotonic()
    try:
        yield
    finally:
        logging.info(f"Duration: {round(time.monotonic() - start)} seconds")


async def fetch_one(product_results, client, url, key, product_id, language):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "product.get",
        "params": {"language": language, "access_key": key, "id": product_id},
    }

    async with await client.post(url, json=payload) as resp:
        resp = await resp.json()
        product_results.append(process_response(resp))


async def fetch_batch(product_list, api_url, api_key, language):
    product_results = []
    async with aiohttp.ClientSession() as client:
        tasks = [
            asyncio.create_task(fetch_one(product_results, client, api_url, api_key, product_id, language))
            for product_id in product_list
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    return product_results

# ignore timeout errors
if __name__ == "__main__":
    kbc_datadir = os.environ.get("KBC_DATADIR")

    utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    logging.basicConfig(format='%(name)s, %(asctime)s, %(levelname)s, %(message)s',
                        level=logging.DEBUG)

    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()

    print(datetime.datetime.now(), 'EXTRACTION BEGINS')
    logging.info("Extracting parameters from config.")

    input_filename = parameters.get("input_filename")
    output_result_filename = parameters.get("output_result_filename")
    output_fails_filename = parameters.get("output_fails_filename")
    product_id_column_name = parameters.get("product_id_column_name")
    wanted_columns = parameters.get("wanted_columns")
    max_attempts = int(parameters.get("max_attempts", "1"))

    # log parameters (excluding sensitive designated by '#')
    logging.info({k: v for k, v in parameters.items() if "#" not in k})
    # read unique product ids
    with open(f'{kbc_datadir}in/tables/{input_filename}.csv') as input_file:
        original_product_ids = {
                int(row[product_id_column_name]) for row in csv.DictReader(input_file)
            }
        product_ids = list(original_product_ids)

    logging.info(f"Input unique products: {len(original_product_ids)}")
    logging.info(f"product_ids sample: {original_product_ids[:5]}")

    with PriceWriter(
                target_file_name=f'{kbc_datadir}out/tables/{output_result_filename}.csv',
                colnames=wanted_columns + ['utctime_started'],
                prod_id_colname='product_id',
            ) as writer:
        with time_logger():
            attempts = defaultdict(int)
            for batch_i, product_batch in enumerate(batches(product_ids, batch_size=9900, window_size=61)):
                logging.info(f"Downloading batch {batch_i}")

                for pid in product_batch:
                    attempts[pid] += 1

                result_list = asyncio.run(fetch_batch(
                    product_list=product_batch,
                    api_url=parameters.get("api_url"),
                    api_key=parameters.get("#api_key"),
                    language=parameters.get("language"),
                ))

                # flatten and transform results
                results = [
                        # filter item columns to only relevant ones and add utctime_started
                        {
                            **{colname: colval for colname, colval in item.items() if colname in wanted_columns},
                            **{'utctime_started': utctime_started}
                        }
                        for sublist in result_list
                        # drop empty sublists or None results
                        if sublist
                        for item in sublist
                    ]

                logging.info(f"Writing batch {batch_i}")
                # append results to the target file
                writer.writerows(results)
                success_ids = {result['product_id'] for result in results}
                failed_ids = set(product_batch).difference(success_ids)
                failed_under_max_attempts = [
                    pid for pid in failed_ids
                    if attempts[pid] < max_attempts
                ]
                product_ids.extend(failed_under_max_attempts)

                logging.info(f'{len(success_ids)} IDs retrieved successfully')
                logging.info(f'{len(failed_ids)} IDs failed')
                logging.info(f'{len(failed_under_max_attempts)} IDs requeued for extraction')


        logging.info(f"Output row #: {writer.total_rows}")
        logging.info(f"Output unique products #: {len(writer.result_products)}")

        # log what was not returned
        missing_products = list(original_product_ids - set(writer.result_products))

    logging.info(f"Missing product #: {len(missing_products)}")
    with open(f'{kbc_datadir}out/tables/{output_fails_filename}.csv', 'w', encoding='utf8') as missf:
        missf.writelines(os.linesep.join(["product_id"] + list(map(str, missing_products))))

    logging.info("Script done.")
