import sys, os, shutil, logging, copy, json
from airbyte_cdk.entrypoint import launch, AirbyteEntrypoint
from .source import SourceMastodonFetcher
from typing import Optional

# def run():
#     args = sys.argv[1:]
#     source = SourceMastodonFetcher()
#     launch(source, args)

def run():
    config_mapping = {
        "tag_feed.json": "tags",
        "account_feed.json": "accounts"
    }
    
    logger = logging.getLogger("airbyte")
    args = sys.argv[1:]
    source = SourceMastodonFetcher()
    logger.info(args)
    if "read" not in args and "discover" not in args:
        launch(source, args)
        return
    
    config_path: Optional[str] = AirbyteEntrypoint(source).extract_config(args)
    catalog_path: Optional[str] = AirbyteEntrypoint(source).extract_catalog(args)
    
    logger.info(f"Config path: {config_path}")
    logger.info(f"Catalog path: {catalog_path}")
    
    config = source.read_config(config_path)
    logger.info(f"Config: {config}")
    stream_names = []
    # (1) Fix schemas for "discover"
    schemas_folder = "./airbyte/integration_code/source_mastodon_fetcher/schemas/"
    for file_name in os.listdir(schemas_folder):
        config_key = config_mapping.get(file_name)
        if not config_key:
            continue

        src_file_path = os.path.join(schemas_folder, file_name)
        # Create schema files
        for value in config[config_key]:
            current_file_name = f"{value}_{file_name}"
            dst_file_path = os.path.join(schemas_folder, current_file_name)
            shutil.copy(src_file_path, dst_file_path)
            stream_names.append(current_file_name.replace(".json", ""))
            logger.info(f"Created {dst_file_path}")

    if "discover" in args:
        launch(source, args)
        return
    
    # (2) Fix Stream names
    with open(catalog_path, "r") as file:
        catalog: dict = json.loads(file.read())
    
    catalog_mappings = {info["stream"]["name"]: info for info in catalog["streams"]}
    new_catalogs = {
        "streams": []
    }
    logger.info(f"Catalog loaded")
    
    for key, current_catalog in catalog_mappings.items():

        value = config_mapping.get(f"{key}.json")
        if not value:
            continue
        
        for stream_name in stream_names:
            
            if key not in stream_name:
                continue
            current = copy.deepcopy(current_catalog)
            current["stream"]["name"] = stream_name
            new_catalogs["streams"].append(current)

    for stream_name in new_catalogs["streams"]:
        logger.info(stream_name)
    
    with open(catalog_path, "w", encoding="utf-8") as file:
        json.dump(new_catalogs, file, indent=4)
    
    logger.info(f"Updated {catalog_path}")

    try:
        launch(source, args)
    except Exception as e:
        logger.error(str(e))
    
    with open(catalog_path, "w", encoding="utf-8") as file:
        json.dump(catalog, file, indent=4)