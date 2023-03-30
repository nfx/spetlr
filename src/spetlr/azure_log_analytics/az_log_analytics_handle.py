import base64
import hashlib
import hmac
import json
import warnings
from datetime import date, datetime
from typing import Any, Dict

import requests
from pyspark.sql import DataFrame


class AZLogAnalyticsHandle:
    def __init__(
        self,
        log_analytics_workspace_id: str,
        shared_key: str,
        log_type: str = "DatabricksLoggingOrchestrator",
    ):
        self.workspace_id = log_analytics_workspace_id
        self.shared_key = shared_key
        self.log_type = log_type

    def _create_uri(self, resource: str) -> str:
        return (
            f"https://{self.workspace_id}.ods.opinsights"
            + f".azure.com{resource}?api-version=2016-04-01"
        )

    def _create_body(self, df: DataFrame) -> str:
        body = df.collect()
        body = [row.asDict() for row in body]
        body = json.dumps(body, indent=4, default=json_serial)

        return body

    def _create_headers(
        self, method: str, content_type: str, content_length: int, resource: str
    ) -> Dict[str, str]:
        data_rfc1123_format = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

        x_headers = f"x-ms-date:{data_rfc1123_format}"
        string_to_hash = (
            f"{method}\n{str(content_length)}\n{content_type}\n{x_headers}\n{resource}"
        )
        bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        authorization = f"SharedKey {self.workspace_id}:{encoded_hash}"

        return {
            "content-type": content_type,
            "Authorization": authorization,
            "Log-Type": self.log_type,
            "x-ms-date": data_rfc1123_format,
        }

    def http_data_collector_api_post(self, df: DataFrame) -> None:
        resource = "/api/logs"

        uri = self._create_uri(resource=resource)
        body = self._create_body(df)
        headers = self._create_headers(
            method="POST",
            resource=resource,
            content_type="application/json",
            content_length=len(body),
        )

        response = requests.post(uri, data=body, headers=headers)

        if response.status_code != 200:
            warnings.warn(
                "Failure to send message to Azure Log Workspace. "
                + f"Response code: {response.status_code}"
            )
        else:
            print(f"Logging API POST method response code: {response.status_code}")

    append = http_data_collector_api_post


def json_serial(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
