from google.cloud import storage
from typing import Any, Optional, Sequence, Union
import logging

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import tempfile
import requests
import pandas as pd

class WebToGCSHKOperator(BaseOperator):
    """
    Move data from a web server link to a GCS bucket
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "service",
        "destination_path",
        "destination_bucket",
    )

    def __init__(
            self,
            *,
            endpoint: str,
            destination_path: Optional[str] = None,
            destination_bucket: Optional[str] = None,
            service: str,
            gcp_conn_id: str = "google_cloud_default",
            gzip: bool = False,
            mime_type: str = "text/csv",
            delegate_to: Optional[str] = None,
            impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.endpoint = self._format_endpoint(endpoint, service, destination_path)
        self.destination_path = self._format_destination_path(destination_path)
        self.destination_bucket = self._format_bucket_name(destination_bucket)
        self.service = service
        self.gcp_conn_id = gcp_conn_id
        self.gzip = gzip
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Any):
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain
        )
        self._copy_file_object(gcs_hook)

    def _copy_file_object(self, gcs_hook: GCSHook) -> None:
        """
        Function to download and copy a file to a GCS bucket
        """
        try:
            self.log.info("Execute downloading of file from %s to gs://%s//%s",
                          self.endpoint,
                          self.destination_bucket,
                          self.destination_path)

            # download the file using requests into a temporary directory and create a pandas dataframe
            with tempfile.TemporaryDirectory() as tmpdirname:
                request_url = self.endpoint
                r = requests.get(request_url)
                open(f'{tmpdirname}/{self.destination_path}', 'wb').write(r.content)
                self.log.info(f"File written to temporary directory: {tmpdirname}/{self.destination_path}")

                # read the CSV file into a pandas dataframe
                df = pd.read_csv(f'{tmpdirname}/{self.destination_path}', encoding='utf-8')
                self.log.debug(df.head())
                self.log.debug(df.shape)

                # modify the file name if needed
                file_name = self.destination_path.replace('.csv.gz', '.csv')
                df.to_csv(f'{tmpdirname}/{file_name}', index=False)

                # local file path
                local_file_name = f'{tmpdirname}/{file_name}'

                # upload the file to GCS using GCS hooks
                gcs_hook.upload(
                    bucket_name=self.destination_bucket,
                    object_name=f"{self.service}/{file_name}",
                    filename=local_file_name,
                    mime_type=self.mime_type,
                    gzip=False,
                )

                self.log.info("Loaded file from %s to gs://%s//%s",
                              self.endpoint,
                              self.destination_bucket,
                              f"{self.service}/{file_name}")

        except Exception as e:
            self.log.error("An error occurred during file processing: %s", str(e))
            # You may want to handle the error or re-raise it based on your use case
            raise

    @staticmethod
    def _format_endpoint(endpoint: Optional[str], service: str, destination_path: str) -> str:
        if endpoint:
            return endpoint
        return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{destination_path}"

    @staticmethod
    def _format_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _format_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")
