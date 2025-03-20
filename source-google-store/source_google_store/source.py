"""
Source implementation for Google Play Store Reviews.
"""
import csv
import logging
from datetime import datetime
from typing import Dict, Generator, Any, List, Mapping, MutableMapping, Optional, Iterable, Tuple

import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode

logger = logging.getLogger("airbyte")


class SourceGoogleStore(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            client_email = config.get("client_email")
            credentials_json = config.get("credentials_json")
            
            if not client_email or not credentials_json:
                return False, "Missing required fields: client_email or credentials_json"
            
            # Create temporary credentials file
            import json
            from tempfile import NamedTemporaryFile
            
            with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(json.loads(credentials_json), temp_file)
                temp_file_path = temp_file.name
            
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                temp_file_path,
                scopes=['https://www.googleapis.com/auth/devstorage.read_only']
            )
            
            # Clean up
            import os
            os.unlink(temp_file_path)
            
            storage = build('storage', 'v1', http=credentials.authorize(httplib2.Http()))
            
            # Get bucket metadata 
            bucket = config.get("bucket")
            storage.buckets().get(bucket=bucket).execute()

            return True, None
        except Exception as e:
            return False, f"Error connecting to Google Play Store: {str(e)}"


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            ReviewsStream(config=config)
        ]

class ReviewsStream(Stream):
    name = "reviews"
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.client_email = config.get("client_email")
        self.json_key_file = config.get("json_key_file")
        self.bucket = config.get("bucket")
        self.package_name = config.get("package_name")
        self.dates = config.get("dates", [])

        if not self.dates:  # Default to last month if not specified
            current_date = datetime.now()
            last_month = f"{current_date.year}{current_date.month-1:02d}" if current_date.month > 1 else f"{current_date.year-1}12"
            self.dates = [last_month] #YYYYMM format


    def get_credentials(self):
        import json
        from tempfile import NamedTemporaryFile
        
        # Get the JSON credentials string from config
        credentials_json = self.config.get("credentials_json")
        
        # Create a temporary file to store the credentials
        with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            # Write the JSON content to the temporary file
            json.dump(json.loads(credentials_json), temp_file)
            temp_file_path = temp_file.name
        
        # Create credentials from the temporary file
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            temp_file_path,
            scopes=['https://www.googleapis.com/auth/devstorage.read_only']
        )
        
        # Clean up the temporary file
        import os
        os.unlink(temp_file_path)
        
        return credentials
    
    def get_storage_client(self):
        credentials = self.get_credentials()
        storage = build('storage', 'v1', http=credentials.authorize(httplib2.Http()))
        return storage

    #Download and parse a report from Google Cloud Storage
    def get_report_data(self, report_path: str) -> List[Dict[str, Any]]:
        storage = self.get_storage_client()
        
        try:
            # Get the file content
            request = storage.objects().get_media(
                bucket=self.bucket,
                object=report_path
            )
            
            # Download and process directly from memory
            content = request.execute()
            from io import BytesIO, TextIOWrapper
            
            # Use BytesIO to handle binary data
            binary_stream = BytesIO(content)
            # Use UTF-16 encoding since the file has UTF-16 BOM
            text_stream = TextIOWrapper(binary_stream, encoding='utf-16')
            
            csv_reader = csv.reader(text_stream)
            headers = next(csv_reader)
            # Clean up the BOM from the first header if present
            if headers and headers[0].startswith('\ufeff'):
                headers[0] = headers[0].replace('\ufeff', '')
            
            json_data = []
            for row in csv_reader:
                record = {}
                for i, header in enumerate(headers):
                    # Clean up the header by removing null bytes and extra spaces
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
                
                record['_metadata'] = {
                    'package_name': self.package_name,
                    'report_path': report_path,
                    'extracted_at': datetime.now().isoformat()
                }
                
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
                for record in self.get_report_data(path):
                    yield record

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """Generate stream slices representing each file one file is a month of data"""
        slices = []
        
        for date in self.dates:
            # For reviews, we use a specific path format
            path = f"reviews/reviews_{self.package_name}_{date}.csv"
            slices.append({"path": path, "date": date})
                
        return slices
        
   