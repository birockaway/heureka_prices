import asyncio
import aiohttp
import time
from keboola import docker
from datetime import datetime
from collections import defaultdict
import os
from contextlib import contextmanager
import pandas as pd


def log(message, level='INFO'):
    timestamp = datetime.now().strftime('%Y%m%d %H:%M:%S.%f')
    print(f'{timestamp} - {level} - {message}')


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
        log('Response does not contain product data.', level='DEBUG')
        log(f"Exception {e}", level='DEBUG')
        log(response_json, level='DEBUG')
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
            log(e, level='EXCEPTION')

            return None


def batches(product_list, batch_size, window_size, sleep_time=5):
    window_start = time.monotonic()
    while product_list:
        batch = product_list[:batch_size]
        del product_list[:batch_size]

        while time.monotonic() - window_start < window_size:
            log('waiting for time window to expire...')
            time.sleep(sleep_time)
        window_start = time.monotonic()
        yield batch


@contextmanager
def time_logger():
    start = time.monotonic()
    try:
        yield
    finally:
        log(f"Duration: {round(time.monotonic() - start)} seconds")


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

    utctime_started = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()

    log("Extracting parameters from config.")

    cse_material_mapping_filename = parameters.get("cse_material_mapping_filename")
    hourly_materials_filename = parameters.get("hourly_materials_filename")
    runs_history_filename = parameters.get("runs_history_filename")
    output_result_filename = parameters.get("output_result_filename")
    output_fails_filename = parameters.get("output_fails_filename")
    product_id_column_name = parameters.get("product_id_column_name")
    wanted_columns = parameters.get("wanted_columns")
    country = parameters.get('country')
    source = parameters.get('source')
    batch_size = parameters.get('batch_size')
    time_window_per_batch = parameters.get('time_window_per_batch')
    max_attempts = parameters.get("max_attempts", 1)

    # log parameters (excluding sensitive designated by '#')
    log({k: v for k, v in parameters.items() if "#" not in k})

    # decide run_type
    runs_history = pd.read_csv(f'{kbc_datadir}in/tables/{runs_history_filename}.csv', parse_dates=['DATETIME'])
    runs_today = runs_history[runs_history['DATETIME'].dt.date == datetime.utcnow().date()]
    run_type = 'HOURLY' if 'DAILY' in runs_today['RUN_TYPE'].unique() else 'DAILY'
    log(f'{run_type} load started.')

    # read unique product ids
    cse_material_map = pd.read_csv(f'{kbc_datadir}in/tables/{cse_material_mapping_filename}.csv', dtype=str)
    cse_material_map = cse_material_map[
        (cse_material_map['country'] == country)
        & (cse_material_map['source'] == source)
        & (cse_material_map['material'] != '')
        & pd.notnull(cse_material_map['material'])
        & (cse_material_map[product_id_column_name] != '')
        & pd.notnull(cse_material_map[product_id_column_name])
    ]

    if run_type == 'HOURLY':
        hourly_materials_df = pd.read_csv(f'{kbc_datadir}in/tables/{hourly_materials_filename}.csv', dtype=str)
        hourly_materials = set(hourly_materials_df['MATERIAL'].unique())
        cse_material_map = cse_material_map[cse_material_map['material'].isin(hourly_materials)]

    original_product_ids = set(cse_material_map[product_id_column_name].astype('int64'))
    product_ids = list(original_product_ids)

    log(f"Input unique products: {len(original_product_ids)}")
    log(f"product_ids sample: {product_ids[:5]}")

    attempts = defaultdict(int)
    written_ids = set()

    with time_logger():
        for batch_i, product_batch in enumerate(batches(product_ids, batch_size=batch_size, window_size=time_window_per_batch)):
            for pid in product_batch:
                attempts[pid] += 1

            log(f"Scraping batch {batch_i}")
            result_list = asyncio.run(fetch_batch(
                product_list=product_batch,
                api_url=parameters.get("api_url"),
                api_key=parameters.get("#api_key"),
                language=parameters.get("language"),
            ))
            log(f"Scraped batch {batch_i}")

            # flatten and transform results
            batch_results = [
                    # filter item columns to only relevant ones and add utctime_started
                    {
                        **{colname: colval for colname, colval in item.items() if colname in wanted_columns},
                        **{'utctime_started': utctime_started}
                    }
                    for sublist in result_list if sublist
                    for item in sublist
                ]
            log(f"Parsed batch {batch_i}")

            batch_output = pd.DataFrame(batch_results, columns=wanted_columns + ['utctime_started'])
            batch_output = batch_output.groupby(['product_id', 'shop_id', 'offer_id'], as_index=False).first()
            success_ids = set(batch_output['product_id'].unique())
            failed_ids = set(product_batch).difference(success_ids)

            if max_attempts > 1:
                failed_under_max_attempts = [
                    pid for pid in failed_ids
                    if attempts[pid] < max_attempts
                ]
                product_ids.extend(failed_under_max_attempts)
            else:
                failed_under_max_attempts = []

            log(f'{len(success_ids)} IDs retrieved successfully')
            log(f'{len(failed_ids)} IDs failed')
            log(f'{len(failed_under_max_attempts)} IDs requeued for extraction')

            log(f'Saving batch {batch_i}')
            batch_output.to_csv(f'{kbc_datadir}out/tables/{output_result_filename}.csv',
                                index=False, mode='a', header=not batch_i, columns=sorted(batch_output.columns))
            written_ids = written_ids.union(success_ids)

    missing_products = list(original_product_ids - written_ids)
    log(f"Output unique products #: {len(written_ids)}")
    log(f"Missing product #: {len(missing_products)}")

    log('Saving runs history')
    run_log = pd.DataFrame(
        {
            'DATETIME': [utctime_started],
            'RUN_TYPE': [run_type],
            'SUCCEEDED_COUNT': [len(written_ids)],
            'FAILED_COUNT': [len(missing_products)]
        }
    )
    run_log['DATETIME'] = pd.to_datetime(run_log['DATETIME'])
    runs_history = pd.concat([runs_today, run_log])
    runs_history.to_csv(f'{kbc_datadir}out/tables/{runs_history_filename}.csv', index=False)

    log('Saving missing products')
    pd.DataFrame({product_id_column_name: missing_products}) \
      .to_csv(f'{kbc_datadir}out/tables/{output_fails_filename}.csv', index=False)

    log("Script done.")
