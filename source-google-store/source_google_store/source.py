"""
Source implementation for Google Play Store Reviews.
"""
import csv
import logging
from datetime import datetime
from typing import Dict, Generator, Any, List, Mapping, MutableMapping, Optional, Iterable, Tuple, Iterator

import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode

import json
import os
from io import BytesIO, TextIOWrapper
from datetime import datetime
from tempfile import NamedTemporaryFile

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
            InstallsOverviewStream(config=config),
            InstallsAndroidVersionStream(config=config),
            InstallsAppVersionStream(config=config),
            InstallsCountryStream(config=config),
            CrashesOverviewStream(config=config),
            CrashesAndroidVersionStream(config=config),
            CrashesAppVersionStream(config=config),
            CrashesDeviceStream(config=config),
            CrashesCountryStream(config=config),
            RatingsOverviewStream(config=config),
            RatingAndroidVersionStream(config=config),
            RatingAppVersionStream(config=config),
            RatingCountryStream(config=config)
        ]

class BaseGooglePlayStream(Stream):
    """Base stream class for all Google Play Store streams"""
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.credentials_json = config.get("credentials_json")
        self.bucket = config.get("bucket")
        self.package_name = config.get("package_name")
        

        self.dates = config.get("dates", [])
        if not self.dates:
            current_date = datetime.now()
  
            dates_list = []
            start_year, start_month = 2024, 8
            
            while (start_year, start_month) <= (current_date.year, current_date.month):
                date_str = f"{start_year}{start_month:02d}"
                dates_list.append(date_str)

                start_month += 1
                if start_month > 12:
                    start_month = 1
                    start_year += 1
            
            self.dates = dates_list
            logging.info(f"Automatically generated dates from August 2024: {self.dates}")

    
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
            
            binary_stream = BytesIO(content)
            text_stream = TextIOWrapper(binary_stream, encoding='utf-16')
            
            csv_reader = csv.reader(text_stream)
            headers = next(csv_reader)
            if headers and headers[0].startswith('\ufeff'):
                headers[0] = headers[0].replace('\ufeff', '')
            
            json_data = []
            for row in csv_reader:
                record = {}
                for i, header in enumerate(headers):
                    clean_header = header.strip().strip('\x00')
                    value = row[i].strip().strip('\x00') if i < len(row) else ""
                    try:
                        if '.' in value:
                            value = float(value)
                        elif value.isdigit():
                            value = int(value)
                    except (ValueError, TypeError):
                        pass
                    
                    record[clean_header] = value
                
                metadata = {
                    'package_name': self.package_name,
                    'report_path': report_path,
                    'extracted_at': datetime.now().isoformat()
                }
                
                if stats_type:
                    metadata['stats_type'] = stats_type
                
                record['_metadata'] = metadata
                
                json_data.append(record)
            
            return json_data
            
        except Exception as e:
            logging.error(f"Error downloading report {report_path}: {str(e)}")
            return []

    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        if stream_slice:
            path = stream_slice.get("path")
            if path:
                for record in self.get_report_data(path, self.get_stats_type()):
                    yield record
    
    def get_stats_type(self) -> str:
        """Return the stats_type for this stream"""
        return None
                    
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

class ReviewsStream(BaseGooglePlayStream):
    name = "reviews"
    schema_file = "reviews.json"
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config, **kwargs)
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
        return f"stats/installs/installs_{{package_name}}_{{date}}_{self.get_stats_suffix()}.csv"
    
    def get_stats_suffix(self) -> str:
        raise NotImplementedError("Subclasses must implement get_stats_suffix")
        
    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        if stream_slice:
            path = stream_slice.get("path")
            if path:
                logging.info(f"Processing installs {self.get_stats_suffix()} report: {path}")
                for record in self.get_report_data(path, self.get_stats_type()):
                    yield record

class CrashesBaseStream(BaseGooglePlayStream):
    def get_path_pattern(self) -> str:
        return f"stats/crashes/crashes_{{package_name}}_{{date}}_{self.get_stats_suffix()}.csv"
    
    def get_stats_suffix(self) -> str:
        raise NotImplementedError("Subclasses must implement get_stats_suffix")
        
    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        if stream_slice:
            path = stream_slice.get("path")
            if path:
                logging.info(f"Processing crashes {self.get_stats_suffix()} report: {path}")
                for record in self.get_report_data(path, self.get_stats_type()):
                    yield record

class RatingsBaseStream(BaseGooglePlayStream):
    def get_path_pattern(self) -> str:
        return f"stats/ratings/ratings_{{package_name}}_{{date}}_{self.get_stats_suffix()}.csv"
    
    def get_stats_suffix(self) -> str:
        raise NotImplementedError("Subclasses must implement get_stats_suffix")
        
    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        if stream_slice:
            path = stream_slice.get("path")
            if path:
                logging.info(f"Processing ratings {self.get_stats_suffix()} report: {path}")
                for record in self.get_report_data(path, self.get_stats_type()):
                    yield record

# Install streams
class InstallsOverviewStream(InstallsBaseStream):
    name = "installs_overview"
    schema_file = "installs_overview.json"
    
    def get_stats_suffix(self) -> str:
        return "overview"
    
    def get_stats_type(self) -> str:
        return "overview"

class InstallsAndroidVersionStream(InstallsBaseStream):
    name = "installs_android_version"
    schema_file = "installs_android_version.json"
    
    def get_stats_suffix(self) -> str:
        return "os_version"
    
    def get_stats_type(self) -> str:
        return "os_version"

class InstallsAppVersionStream(InstallsBaseStream):
    name = "installs_app_version"
    schema_file = "installs_app_version.json"
    
    def get_stats_suffix(self) -> str:
        return "app_version"
    
    def get_stats_type(self) -> str:
        return "app_version"

class InstallsCountryStream(InstallsBaseStream):
    name = "installs_country"
    schema_file = "installs_country.json"
    
    def get_stats_suffix(self) -> str:
        return "country"
    
    def get_stats_type(self) -> str:
        return "country"

# Crashes streams
class CrashesOverviewStream(CrashesBaseStream):
    name = "crashes_overview"
    schema_file = "crashes_overview.json"
    
    def get_stats_suffix(self) -> str:
        return "overview"
    
    def get_stats_type(self) -> str:
        return "overview"

class CrashesAndroidVersionStream(CrashesBaseStream):
    name = "crashes_android_version"
    schema_file = "crashes_android_version.json"
    
    def get_stats_suffix(self) -> str:
        return "os_version"
    
    def get_stats_type(self) -> str:
        return "os_version"

class CrashesAppVersionStream(CrashesBaseStream):
    name = "crashes_app_version"
    schema_file = "crashes_app_version.json"
    
    def get_stats_suffix(self) -> str:
        return "app_version"
    
    def get_stats_type(self) -> str:
        return "app_version"

class CrashesDeviceStream(CrashesBaseStream):
    name = "crashes_device"
    schema_file = "crashes_device.json"
    
    def get_stats_suffix(self) -> str:
        return "device"
    
    def get_stats_type(self) -> str:
        return "device"

class CrashesCountryStream(CrashesBaseStream):
    name = "crashes_country"
    schema_file = "crashes_country.json"
    
    def get_stats_suffix(self) -> str:
        return "country"
    
    def get_stats_type(self) -> str:
        return "country"

# Ratings streams
class RatingsOverviewStream(RatingsBaseStream):
    name = "ratings_overview"
    schema_file = "ratings_overview.json"
    
    def get_stats_suffix(self) -> str:
        return "overview"
    
    def get_stats_type(self) -> str:
        return "overview"

class RatingAndroidVersionStream(RatingsBaseStream):
    name = "rating_android_version"
    schema_file = "rating_android_version.json"
    
    def get_stats_suffix(self) -> str:
        return "os_version"
    
    def get_stats_type(self) -> str:
        return "os_version"

class RatingAppVersionStream(RatingsBaseStream):
    name = "rating_app_version"
    schema_file = "rating_app_version.json"
    
    def get_stats_suffix(self) -> str:
        return "app_version"
    
    def get_stats_type(self) -> str:
        return "app_version"

class RatingCountryStream(RatingsBaseStream):
    name = "rating_country"
    schema_file = "rating_country.json"
    
    def get_stats_suffix(self) -> str:
        return "country"
    
    def get_stats_type(self) -> str:
        return "country"
