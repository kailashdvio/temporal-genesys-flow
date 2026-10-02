import json
import logging
import types
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Union, get_args, get_origin

import aiohttp
import pandas as pd
from temporalio import activity

from app.config.settings import settings


@dataclass
class GenesysConversation:
    conversationId: str
    conversationStart: str | None
    conversationEnd: str | None
    raw_data: dict | None


@dataclass
class GenesysOutboundCampaign:
    id: str
    name: str | None
    campaignStatus: str | None
    raw_data: dict | None


@dataclass
class GenesysOutboundCampaignStats:
    campaignId: str
    raw_data: dict | None


class SnowflakeType(str, Enum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    VARIANT = "VARIANT"
    ARRAY = "ARRAY"

    @classmethod
    def from_python_type(cls, python_type):
        mapping = {
            str: cls.STRING,
            int: cls.NUMBER,
            float: cls.FLOAT,
            bool: cls.BOOLEAN,
            date: cls.DATE,
            datetime: cls.TIMESTAMP_NTZ,
            dict: cls.VARIANT,
            list: cls.ARRAY,
        }
        return mapping.get(python_type, cls.VARIANT)


def _resolve_python_type(field_type):
    origin = get_origin(field_type)

    if origin is None:
        return field_type

    if origin is Union or (hasattr(types, "UnionType") and origin is types.UnionType):
        args = [arg for arg in get_args(field_type) if arg is not type(None)]
        return _resolve_python_type(args[0]) if args else Any

    return origin


def create_snowflake_table_from_dataclass(
    conn,
    schema_name: str,
    table_name: str,
    model_cls,
    exclude_fields: list[str] | None = None,
):
    exclude_fields = exclude_fields or []
    columns = []

    for field in fields(model_cls):
        if field.name in exclude_fields:
            continue

        python_type = _resolve_python_type(field.type)
        if field.name.lower() in ["date", "updated_date", "created_time", "conversationstart", "conversationend"]:
            snowflake_type = "TIMESTAMP_NTZ"
        else:
            snowflake_type = SnowflakeType.from_python_type(python_type).value

        columns.append(f'"{field.name.upper()}" {snowflake_type}')

    if not columns:
        raise ValueError("No columns found for table creation.")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}."{table_name}" (
        {", ".join(columns)}
    )
    """

    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cursor.execute(create_table_sql)


def delete_existing_records(conn, schema_name, table_name, df, id_col: str):
    if df is None or df.empty:
        return

    if id_col.upper() not in df.columns:
        return

    unique_ids = df[id_col.upper()].dropna().unique()
    if len(unique_ids) == 0:
        return

    cs = conn.cursor()
    try:
        cs.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name.upper()}'
            AND TABLE_NAME = '{table_name.upper()}'
        """)
        table_exists = cs.fetchone()[0] > 0
        if not table_exists:
            return

        escaped_ids = [str(i).replace("'", "''") for i in unique_ids]
        id_list = ", ".join([f"'{eid}'" for eid in escaped_ids])
        cs.execute(f'DELETE FROM {schema_name}."{table_name}" WHERE "{id_col.upper()}" IN ({id_list})')

    except Exception as e:
        logging.error(f"Failed to delete existing records for {table_name}: {e}")
        raise
    finally:
        cs.close()


GENESYS_REGION_DOMAIN_MAP = {
    "Americas (US East)": "mypurecloud.com",
    "Americas (US East 2)": "use2.us-gov-pure.cloud",
    "Americas (US West)": "usw2.pure.cloud",
    "Americas (Canada)": "cac1.pure.cloud",
    "Americas (São Paulo)": "sae1.pure.cloud",
    "EMEA (Frankfurt)": "mypurecloud.de",
    "EMEA (Dublin)": "mypurecloud.ie",
    "EMEA (London)": "euw2.pure.cloud",
    "Asia Pacific (Mumbai)": "aps1.pure.cloud",
    "Asia Pacific (Seoul)": "apne2.pure.cloud",
    "Asia Pacific (Sydney)": "mypurecloud.com.au",
}


async def _get_genesys_access_token(client_id: str, client_secret: str, tenant_endpoint: str) -> str:
    domain = GENESYS_REGION_DOMAIN_MAP.get(tenant_endpoint, "mypurecloud.com")
    url = f"https://login.{domain}/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            if not resp.ok:
                raise RuntimeError(f"Failed to authenticate with Genesys: {await resp.text()}")
            res = await resp.json()
            return res["access_token"]


def _get_api_base_url(tenant_endpoint: str) -> str:
    domain = GENESYS_REGION_DOMAIN_MAP.get(tenant_endpoint, "mypurecloud.com")
    return f"https://api.{domain}"


async def _load_to_snowflake(team_id: str, table_name: str, model_cls, data: list[dict], id_col: str):
    from snowflake.connector.pandas_tools import write_pandas
    from app.config.snowflake_connection import create_snowflake_connection

    if not data:
        return

    df = pd.DataFrame(data)
    schema_name = f"TEAM_{team_id}_SCHEMA"

    conn = create_snowflake_connection()
    cs = conn.cursor()
    try:
        create_snowflake_table_from_dataclass(conn, schema_name, table_name, model_cls)
        cs.execute(f"USE SCHEMA {schema_name}")

        df.columns = [c.upper() for c in df.columns]

        delete_existing_records(conn, schema_name, table_name, df, id_col)

        # Convert dict columns to json strings for Snowflake compatibility
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            overwrite=False,
        )
    finally:
        cs.close()
        conn.close()


# -------------------------------------------------------------
# Activity Definitions
# -------------------------------------------------------------

@activity.defn
async def get_analytics_conversations(creds) -> list[dict]:
    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")
    tenant_endpoint = creds.get("tenant_endpoint")

    since_date = creds.get("since_date")
    until_date = creds.get("until_date")

    if isinstance(since_date, datetime):
        since_str = since_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    elif isinstance(since_date, str):
        since_str = since_date if "T" in since_date else f"{since_date}T00:00:00.000Z"
    else:
        since_str = (datetime.now() - timedelta(days=29)).strftime("%Y-%m-%dT00:00:00.000Z")

    if isinstance(until_date, datetime):
        until_str = until_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    elif isinstance(until_date, str):
        until_str = until_date if "T" in until_date else f"{until_date}T23:59:59.000Z"
    else:
        until_str = datetime.now().strftime("%Y-%m-%dT23:59:59.000Z")

    interval = f"{since_str}/{until_str}"

    token = await _get_genesys_access_token(client_id, client_secret, tenant_endpoint)
    base_url = _get_api_base_url(tenant_endpoint)
    url = f"{base_url.rstrip('/')}/api/v2/analytics/conversations/details/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    all_conversations = []
    page_number = 1
    page_size = 100
    
    async with aiohttp.ClientSession() as session:
        while True:
            payload = {
                "interval": interval,
                "paging": {
                    "pageSize": page_size,
                    "pageNumber": page_number
                }
            }
            async with session.post(url, headers=headers, json=payload) as response:
                if not response.ok:
                    raise RuntimeError(f"Error fetching conversation details: {await response.text()}")
                data = await response.json()
                conversations = data.get("conversations", [])
                if not conversations:
                    break
                all_conversations.extend(conversations)
                if len(conversations) < page_size:
                    break
                page_number += 1
    return all_conversations


@activity.defn
async def load_analytics_conversations_to_snowflake(args):
    team_id, data = args[0], args[1]
    processed = [
        asdict(GenesysConversation(
            conversationId=item.get("conversationId"),
            conversationStart=item.get("conversationStart"),
            conversationEnd=item.get("conversationEnd"),
            raw_data=item
        ))
        for item in data
    ]
    await _load_to_snowflake(team_id, "GENESYS_CONVERSATIONS", GenesysConversation, processed, "conversationId")


@activity.defn
async def get_outbound_campaigns(creds) -> list[dict]:
    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")
    tenant_endpoint = creds.get("tenant_endpoint")

    token = await _get_genesys_access_token(client_id, client_secret, tenant_endpoint)
    base_url = _get_api_base_url(tenant_endpoint)
    url = f"{base_url.rstrip('/')}/api/v2/outbound/campaigns"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    all_campaigns = []
    page_size = 100
    
    async with aiohttp.ClientSession() as session:
        while url:
            params = {"pageSize": page_size}
            async with session.get(url, headers=headers, params=params if "?" not in url else None) as response:
                if not response.ok:
                    raise RuntimeError(f"Error fetching outbound campaigns: {await response.text()}")
                data = await response.json()
                all_campaigns.extend(data.get("entities", []))

                next_uri = data.get("nextUri")
                if next_uri:
                    if next_uri.startswith("http"):
                        url = next_uri
                    else:
                        url = f"{base_url.rstrip('/')}{next_uri}"
                else:
                    url = None
    return all_campaigns


@activity.defn
async def load_outbound_campaigns_to_snowflake(args):
    team_id, data = args[0], args[1]
    processed = [
        asdict(GenesysOutboundCampaign(
            id=item.get("id"),
            name=item.get("name"),
            campaignStatus=item.get("campaignStatus"),
            raw_data=item
        ))
        for item in data
    ]
    await _load_to_snowflake(team_id, "GENESYS_CAMPAIGNS", GenesysOutboundCampaign, processed, "id")


@activity.defn
async def get_outbound_campaign_stats(args) -> list[dict]:
    creds, campaign_ids = args[0], args[1]
    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")
    tenant_endpoint = creds.get("tenant_endpoint")

    token = await _get_genesys_access_token(client_id, client_secret, tenant_endpoint)
    base_url = _get_api_base_url(tenant_endpoint)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    all_stats = []
    async with aiohttp.ClientSession() as session:
        for campaign_id in campaign_ids:
            url = f"{base_url.rstrip('/')}/api/v2/outbound/campaigns/{campaign_id}/stats"
            async with session.get(url, headers=headers) as response:
                if response.status == 404:
                    continue
                if not response.ok:
                    raise RuntimeError(f"Error fetching campaign stats for {campaign_id}: {await response.text()}")
                data = await response.json()
                data["campaignId"] = campaign_id
                all_stats.append(data)
    return all_stats


@activity.defn
async def load_outbound_campaign_stats_to_snowflake(args):
    team_id, data = args[0], args[1]
    processed = [
        asdict(GenesysOutboundCampaignStats(
            campaignId=item.get("campaignId"),
            raw_data=item
        ))
        for item in data
    ]
    await _load_to_snowflake(team_id, "GENESYS_CAMPAIGN_STATS", GenesysOutboundCampaignStats, processed, "campaignId")
