"""
Source implementation for Google Play Store Reviews.
"""
import logging
from datetime import datetime
from typing import Dict, Generator, Any, List, Mapping, MutableMapping, Optional, Iterable, Tuple, Iterator

import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode, AirbyteCatalog, AirbyteMessage, ConfiguredAirbyteCatalog
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.models import AirbyteStream

import json
import os
from io import BytesIO, TextIOWrapper
import csv
from tempfile import NamedTemporaryFile
from .utils import clean_report

logger = logging.getLogger("airbyte")

class SourceGoogleStore(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            client_email = config.get("client_email")
            credentials_json = config.get("credentials_json")
            
            if not client_email or not credentials_json:
                return False, "Missing required fields: client_email or credentials_json"
            
            with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(json.loads(credentials_json), temp_file)
                temp_file_path = temp_file.name
            
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                temp_file_path,
                scopes=['https://www.googleapis.com/auth/devstorage.read_only']
            )
            
            os.unlink(temp_file_path)
            
            storage = build('storage', 'v1', http=credentials.authorize(httplib2.Http()))
            
            bucket = config.get("bucket")
            storage.buckets().get(bucket=bucket).execute()

            return True, None
        except Exception as e:
            return False, f"Error connecting to Google Play Store: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            ReviewsStream(config=config),
            InstallsBaseStream(config=config, name="installs_overview", schema_file="installs_overview.json", stats_suffix="overview", stats_type="overview"),
            InstallsBaseStream(config=config, name="installs_android_version", schema_file="installs_android_version.json", stats_suffix="os_version", stats_type="os_version"),
            InstallsBaseStream(config=config, name="installs_app_version", schema_file="installs_app_version.json", stats_suffix="app_version", stats_type="app_version"),
            InstallsBaseStream(config=config, name="installs_country", schema_file="installs_country.json", stats_suffix="country", stats_type="country"),
            CrashesBaseStream(config=config, name="crashes_overview", schema_file="crashes_overview.json", stats_suffix="overview", stats_type="overview"),
            CrashesBaseStream(config=config, name="crashes_android_version", schema_file="crashes_android_version.json", stats_suffix="os_version", stats_type="os_version"),
            CrashesBaseStream(config=config, name="crashes_app_version", schema_file="crashes_app_version.json", stats_suffix="app_version", stats_type="app_version"),
            RatingsBaseStream(config=config, name="ratings_overview", schema_file="ratings_overview.json", stats_suffix="overview", stats_type="overview"),
            RatingsBaseStream(config=config, name="rating_android_version", schema_file="rating_android_version.json", stats_suffix="os_version", stats_type="os_version"),
            RatingsBaseStream(config=config, name="rating_app_version", schema_file="rating_app_version.json", stats_suffix="app_version", stats_type="app_version"),
            RatingsBaseStream(config=config, name="rating_country", schema_file="rating_country.json", stats_suffix="country", stats_type="country")
        ]

class BaseGooglePlayStream(Stream):
    """Base stream class for all Google Play Store streams"""
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], name: str = None, schema_file: str = None, 
                 stats_suffix: str = None, stats_type: str = None, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.credentials_json = config.get("credentials_json")
        self.bucket = config.get("bucket")
        self.package_name = config.get("package_name")
        
        if name:
            self._name = name
        self.schema_file = schema_file
        self.stats_suffix = stats_suffix
        self.stats_type = stats_type

        self.dates = self._generate_dates()

    @property
    def name(self) -> str:
        return self._name if hasattr(self, '_name') else self.__class__.__name__.lower()

    def get_json_schema(self) -> Mapping[str, Any]:
        schema_path = os.path.join(os.path.dirname(__file__), "schemas", self.schema_file)
        logging.info(f"Looking for schema file at: {schema_path}")
        
        if not os.path.exists(schema_path):
            logging.error(f"Schema file does not exist: {schema_path}")

            schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
            if os.path.exists(schema_dir):
                logging.info(f"Available schema files: {os.listdir(schema_dir)}")
            else:
                logging.error(f"Schema directory does not exist: {schema_dir}")
            
            # Return a default schema as fallback
            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "Date": {"type": "string"},
                    "Package Name": {"type": "string"},
                    "_metadata": {"type": "object"}
                }
            }
            
        try:
            with open(schema_path, "r") as file:
                schema = json.load(file)
                logging.info(f"Successfully loaded schema from {schema_path}")
                return schema
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error in schema file {schema_path}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error loading schema from {schema_path}: {str(e)}")
            raise

    def get_storage_client(self):
        credentials_json = self.config.get("credentials_json")
        
        with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(json.loads(credentials_json), temp_file)
            temp_file_path = temp_file.name
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            temp_file_path,
            scopes=['https://www.googleapis.com/auth/devstorage.read_only']
        )
        
        os.unlink(temp_file_path)
        storage = build('storage', 'v1', http=credentials.authorize(httplib2.Http()))
        return storage

    def get_report_data(self, report_path: str, stats_type: str = None) -> List[Dict[str, Any]]:
        storage = self.get_storage_client()
        
        try:
            request = storage.objects().get_media(
                bucket=self.bucket,
                object=report_path
            )
            
            content = request.execute()
            return clean_report(content, self.package_name, report_path, stats_type)
            
        except Exception as e:
            logging.error(f"Error downloading report {report_path}: {str(e)}")
            return []

    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        if stream_slice:
            path = stream_slice.get("path")
            if path:
                logging.info(f"Processing {self.name} report: {path}")
                for record in self.get_report_data(path, self.stats_type):
                    yield record
    
    def get_stats_type(self) -> str:
        return self.stats_type
                    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """one file is a month of data"""
        slices = []
        
        for date in self.dates:
            path = self.get_path_pattern().format(
                package_name=self.package_name,
                date=date
            )
            slices.append({
                "path": path, 
                "date": date
            })
                
        return slices
    
    def get_path_pattern(self) -> str:
        raise NotImplementedError("Subclasses must implement get_path_pattern")

    def _generate_dates(self) -> List[str]:
        """Generate list of dates from start_date until current month"""
        start_date = self.config.get("start_date")
        if not start_date:
            # Default to last month if no start date provided
            current_date = datetime.now()
            if current_date.month == 1:
                start_date = f"{current_date.year-1}12"
            else:
                start_date = f"{current_date.year}{current_date.month-1:02d}"
        
        # Parse start date
        start_year = int(start_date[:4])
        start_month = int(start_date[4:])
        
        # Generate all dates until current month
        current_date = datetime.now()
        dates_list = []
        
        while (start_year, start_month) <= (current_date.year, current_date.month):
            dates_list.append(f"{start_year}{start_month:02d}")
            
            start_month += 1
            if start_month > 12:
                start_month = 1
                start_year += 1
        
        logging.info(f"Generated dates from {dates_list[0]} to {dates_list[-1]}")
        return dates_list

class ReviewsStream(BaseGooglePlayStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(
            config=config,
            name="reviews",
            schema_file="reviews.json",
            **kwargs
        )
        self.json_key_file = config.get("json_key_file")
    
    def get_path_pattern(self) -> str:
        return "reviews/reviews_{package_name}_{date}.csv"
    
    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        if stream_slice:
            path = stream_slice.get("path")
            if path:
                for record in self.get_report_data(path):
                    yield record

class InstallsBaseStream(BaseGooglePlayStream):
    def get_path_pattern(self) -> str:
        return f"stats/installs/installs_{{package_name}}_{{date}}_{self.stats_suffix}.csv"

class CrashesBaseStream(BaseGooglePlayStream):
    def get_path_pattern(self) -> str:
        return f"stats/crashes/crashes_{{package_name}}_{{date}}_{self.stats_suffix}.csv"

class RatingsBaseStream(BaseGooglePlayStream):
    def get_path_pattern(self) -> str:
        return f"stats/ratings/ratings_{{package_name}}_{{date}}_{self.stats_suffix}.csv"