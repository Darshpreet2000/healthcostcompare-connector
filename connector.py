import json
from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests

CMS_HEALTHCARE_DATA_URL = "https://data.cms.gov/data-api/v1/dataset/690ddc6c-2767-4618-b277-420ffb2bf27c/data"
HOSPITAL_DATA_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"

CMS_HEALTHCARE_TABLE_NAME = "cms_healthcare_data"
HOSPITAL_DATA_TABLE_NAME = "hospital_data"

def get_data_cms_healthcare():
    """
    Fetches data from the original CMS API directly using requests with full browser-like headers.
    """
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-US;q=0.9,en;q=0.8',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        # The 'Cookie' header is highly dynamic and should generally not be hardcoded.
        # If it's strictly necessary and static, it can be added, but it's usually
        # managed by a session or is specific to a single browser session.
        # For now, I will omit it to avoid issues with expired or invalid cookies.
        # 'cookie': 'CONSENTMGR=c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:1%7Cc6:1%7Cc7:1%7Cc8:1%7Cc9:1%7Cc10:1%7Cc11:1%7Cc12:1%7Cc13:1%7Cc14:1%7Cc15:1%7Cc16:1%7Cts:1759659224773%7Cconsent:true; AMCV_0600459D5DBAF9400A495E7C%40AdobeOrg=MCMID|03353961221886462893456384165288588255; cms_fpid=86f6fd60-858c-d5a7-32b4-57069388cb53; _ga=GA1.1.364044792.1761047651; ak_bmsc=C591DB13A8C91A4CB1DB7CB133BE71FA~000000000000000000000000000000~YAAQlHUsMWcz2QiaAQAAGkYtEh3VYqsIiRMCIyXP3byLGfPNMpIsEOqfLuNXbQErDXszzBU4Oa7HVITMQzvNx4JBA9P4bmGe9gPVtqo+6Zthrs7uOo5uuFsnMHHu9QtmtTNGtDDiEfDH2j6QzAwxJZMeLc2fds5AJwuUthdQ1Z+wecKe22z1DkJi0gdI1/y7Kc4CDQkGirMAtB4hbGw9mjgHU7/22vtfuX1VIDOY9H4nLcMCbhnaOhxBHyWfeHPDbxXCKlkel3JjiFk1ITiMRXuW2AkrgO0kRn/3hulD8soUX2FnKMbfD3KrIHe9IziysWI/kox58oTGmNo/Vtd8H2Ob6NesSkyqhZZIM+kbtk+KqtEdk9gGMZ/zDJsNHH1BGEJ3r7SXH727vg1msJEGzIlElGhzuEctwID+pLRQNnlz; kndctr_0600459D5DBAF9400A495E7C_AdobeOrg_identity=CiYwMzM1Mzk2MTIyMTg4NjQ2Mjg5MzQ1NjM4NDE2NTI4ODU4ODI1NVIRCPuC9J6bMxgBKgRJTkQxMAHwAavDw5GhMw%3D%3D; kndctr_0600459D5DBAF9400A495E7C_AdobeOrg_cluster=ind1; _ga_V4H982QG9P=GS2.1.s1761242090$o9$g1$t1761242097$j53$l0$h0; _ga_CSLL4ZEK4L=GS2.1.s1761241785$o11$g1$t1761242097$j55$l0$h0; utag_main=v_id:0199b3dceec7000c8f297879c30205075008c06d009bd$_sn:7$_se:19$_ss:0$_st:1761243900746$ses_id:1761241784606%3Bexp-session$_pn:2%3Bexp-session; bm_sv=CE5B9F19859C09EC23157F9FF362CA47~YAAQPNjIFzwcv/iZAQAATrc/Eh2x8qb2FerABby3xgTtalOLaO4cFWivyymqVcAs7kz1zeM8tgv3DBvZVxvCbWDhuIIvM4M+UpbA5zk8MCCqCahB4/fUeHMwdjHYliKvdpm/dBBVv9JytZ/NPiiTJywYVwG6pwaS2m8dzbXS8DwckYlw7UsjKzgIDOxwDL21lkTrWOgG1dF9kwjmkeyEDX4hYKVTUgALya7+5c2i6jd/nJdKvuB466PUqkh/6w==~1'
    }
    log.info(f"Fetching data from {CMS_HEALTHCARE_DATA_URL} with browser-like headers using requests.")
    try:
        response = requests.get(CMS_HEALTHCARE_DATA_URL, headers=headers, timeout=60) # Increased timeout
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json() # Expecting JSON directly
        log.info(f"Received {len(data)} records from CMS Healthcare API.")
        return data
    except requests.exceptions.RequestException as e:
        log.info(f"Error fetching data from CMS Healthcare API using requests: {e}")
        raise Exception(f"Failed to fetch data from CMS Healthcare API using requests: {e}")

def get_data_hospital_data():
    """
    Fetches data from the new CMS API directly using requests with full browser-like headers.
    """
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-US;q=0.9,en;q=0.8',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        # Omitting 'cookie' header for the same reasons as get_data_cms_healthcare
    }
    log.info(f"Fetching data from {HOSPITAL_DATA_URL} with browser-like headers using requests.")
    try:
        response = requests.get(HOSPITAL_DATA_URL, headers=headers, timeout=60) # Increased timeout
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json() # Expecting JSON directly
        records = data.get('results', [])
        log.info(f"Received {len(records)} records from Hospital Data API.")
        return records
    except requests.exceptions.RequestException as e:
        log.info(f"Error fetching data from Hospital Data API using requests: {e}")
        raise Exception(f"Failed to fetch data from Hospital Data API using requests: {e}")

def schema(config):
    """
    Defines the schema for both BigQuery tables.
    """
    return [
        {
            "table": CMS_HEALTHCARE_TABLE_NAME,
            "primary_key": ["Rndrng_Prvdr_CCN", "DRG_Cd"],
            "columns": {
                "Rndrng_Prvdr_CCN": "STRING",
                "Rndrng_Prvdr_Org_Name": "STRING",
                "Rndrng_Prvdr_City": "STRING",
                "Rndrng_Prvdr_St": "STRING",
                "Rndrng_Prvdr_State_FIPS": "STRING",
                "Rndrng_Prvdr_Zip5": "STRING",
                "Rndrng_Prvdr_State_Abrvtn": "STRING",
                "Rndrng_Prvdr_RUCA": "STRING",
                "Rndrng_Prvdr_RUCA_Desc": "STRING",
                "DRG_Cd": "STRING",
                "DRG_Desc": "STRING",
                "Tot_Dschrgs": "STRING",
                "Avg_Submtd_Cvrd_Chrg": "STRING",
                "Avg_Tot_Pymt_Amt": "STRING",
                "Avg_Mdcr_Pymt_Amt": "STRING",
            }
        },
        {
            "table": HOSPITAL_DATA_TABLE_NAME,
            "primary_key": ["facility_id"],
            "columns": {
                "facility_id": "STRING",
                "facility_name": "STRING",
                "address": "STRING",
                "citytown": "STRING",
                "state": "STRING",
                "zip_code": "STRING",
                "countyparish": "STRING",
                "telephone_number": "STRING",
                "hospital_type": "STRING",
                "hospital_ownership": "STRING",
                "emergency_services": "STRING",
                "meets_criteria_for_birthing_friendly_designation": "STRING",
                "hospital_overall_rating": "STRING",
                "hospital_overall_rating_footnote": "STRING",
                "mort_group_measure_count": "STRING",
                "count_of_facility_mort_measures": "STRING",
                "count_of_mort_measures_better": "STRING",
                "count_of_mort_measures_no_different": "STRING",
                "count_of_mort_measures_worse": "STRING",
                "mort_group_footnote": "STRING",
                "safety_group_measure_count": "STRING",
                "count_of_facility_safety_measures": "STRING",
                "count_of_safety_measures_better": "STRING",
                "count_of_safety_measures_no_different": "STRING",
                "count_of_safety_measures_worse": "STRING",
                "safety_group_footnote": "STRING",
                "readm_group_measure_count": "STRING",
                "count_of_facility_readm_measures": "STRING",
                "count_of_readm_measures_better": "STRING",
                "count_of_readm_measures_no_different": "STRING",
                "count_of_readm_measures_worse": "STRING",
                "readm_group_footnote": "STRING",
                "pt_exp_group_measure_count": "STRING",
                "count_of_facility_pt_exp_measures": "STRING",
                "pt_exp_group_footnote": "STRING",
                "te_group_measure_count": "STRING",
                "count_of_facility_te_measures": "STRING",
                "te_group_footnote": "STRING"
            }
        }
    ]

def update(configuration, state):
    """
    Fetches data from both URLs, transforms it, and yields records to Fivetran.
    """
    log.info("Starting data sync for CMS Healthcare and Hospital Data.")
    try:
        # Fetch and process CMS Healthcare Data (using requests)
        cms_healthcare_records = get_data_cms_healthcare()
        log.info(f"Fetched {len(cms_healthcare_records)} records from CMS Healthcare API.")

        for i, record_data in enumerate(cms_healthcare_records):
            # Data types are now handled as STRING based on the updated schema, no conversions needed here.
            op.upsert(table=CMS_HEALTHCARE_TABLE_NAME, data=record_data)
            if (i + 1) % 100 == 0:
                log.info(f"Processed {i + 1} CMS Healthcare records.")

        # Fetch and process Hospital Data (using requests)
        hospital_data_records = get_data_hospital_data()
        log.info(f"Fetched {len(hospital_data_records)} records from Hospital Data API.")

        for i, record_data in enumerate(hospital_data_records):
            # All fields are treated as STRING based on the new schema, so no type conversions are needed here.
            op.upsert(table=HOSPITAL_DATA_TABLE_NAME, data=record_data)
            if (i + 1) % 100 == 0:
                log.info(f"Processed {i + 1} Hospital Data records.")

        log.info("Data sync completed successfully for both sources.")
        op.checkpoint(state)
    except Exception as e:
        log.info(f"An error occurred during sync: {e}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
