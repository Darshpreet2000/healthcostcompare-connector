# Fivetran CMS Healthcare Data Connector

## Overview

This project implements a Fivetran custom connector designed to extract and synchronize healthcare data from two distinct CMS (Centers for Medicare & Medicaid Services) APIs. The connector facilitates the automated ingestion of public healthcare data into a data warehouse, making it accessible for analysis and reporting.

## Features

*   **Automated Data Extraction:** Fetches data from specified CMS API endpoints.
*   **Fivetran SDK Integration:** Built using the Fivetran Connector SDK for seamless integration with the Fivetran platform.
*   **Schema Definition:** Defines clear schemas for the extracted data, including primary keys for reliable upsert operations.
*   **Two Data Sources:** Integrates data from both general CMS healthcare data and specific hospital information.

## Data Sources

This connector extracts data from the following public CMS APIs:

*   **CMS Healthcare Data:** `https://data.cms.gov/data-api/v1/dataset/690ddc6c-2767-4618-b277-420ffb2bf27c/data`
*   **Hospital General Information:** `https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false`

## Schema

The connector defines two tables with the following schemas:

### `cms_healthcare_data`

| Column Name             | Type   | Primary Key | Description                               |
| :---------------------- | :----- | :---------- | :---------------------------------------- |
| `Rndrng_Prvdr_CCN`      | STRING | Yes         | Rendering Provider CCN                    |
| `DRG_Cd`                | STRING | Yes         | DRG Code                                  |
| `Rndrng_Prvdr_Org_Name` | STRING | No          | Rendering Provider Organization Name      |
| `Rndrng_Prvdr_City`     | STRING | No          | Rendering Provider City                   |
| `Rndrng_Prvdr_St`       | STRING | No          | Rendering Provider State                  |
| `Rndrng_Prvdr_State_FIPS` | STRING | No          | Rendering Provider State FIPS Code        |
| `Rndrng_Prvdr_Zip5`     | STRING | No          | Rendering Provider Zip Code               |
| `Rndrng_Prvdr_State_Abrvtn` | STRING | No          | Rendering Provider State Abbreviation     |
| `Rndrng_Prvdr_RUCA`     | STRING | No          | Rendering Provider RUCA Code              |
| `Rndrng_Prvdr_RUCA_Desc` | STRING | No          | Rendering Provider RUCA Description       |
| `DRG_Desc`              | STRING | No          | DRG Description                           |
| `Tot_Dschrgs`           | STRING | No          | Total Discharges                          |
| `Avg_Submtd_Cvrd_Chrg`  | STRING | No          | Average Submitted Covered Charges         |
| `Avg_Tot_Pymt_Amt`      | STRING | No          | Average Total Payment Amount              |
| `Avg_Mdcr_Pymt_Amt`     | STRING | No          | Average Medicare Payment Amount           |

### `hospital_data`

| Column Name                                  | Type   | Primary Key | Description                                   |
| :------------------------------------------- | :----- | :---------- | :-------------------------------------------- |
| `facility_id`                                | STRING | Yes         | Facility ID                                   |
| `facility_name`                              | STRING | No          | Facility Name                                 |
| `address`                                    | STRING | No          | Address                                       |
| `citytown`                                   | STRING | No          | City/Town                                     |
| `state`                                      | STRING | No          | State                                         |
| `zip_code`                                   | STRING | No          | Zip Code                                      |
| `countyparish`                               | STRING | No          | County/Parish                                 |
| `telephone_number`                           | STRING | No          | Telephone Number                              |
| `hospital_type`                              | STRING | No          | Hospital Type                                 |
| `hospital_ownership`                         | STRING | No          | Hospital Ownership                            |
| `emergency_services`                         | STRING | No          | Emergency Services Availability               |
| `meets_criteria_for_birthing_friendly_designation` | STRING | No          | Birthing Friendly Designation Criteria Met    |
| `hospital_overall_rating`                    | STRING | No          | Hospital Overall Rating                       |
| `hospital_overall_rating_footnote`           | STRING | No          | Hospital Overall Rating Footnote              |
| `mort_group_measure_count`                   | STRING | No          | Mortality Group Measure Count                 |
| `count_of_facility_mort_measures`            | STRING | No          | Count of Facility Mortality Measures          |
| `count_of_mort_measures_better`              | STRING | No          | Count of Mortality Measures Better            |
| `count_of_mort_measures_no_different`        | STRING | No          | Count of Mortality Measures No Different      |
| `count_of_mort_measures_worse`               | STRING | No          | Count of Mortality Measures Worse             |
| `mort_group_footnote`                        | STRING | No          | Mortality Group Footnote                      |
| `safety_group_measure_count`                 | STRING | No          | Safety Group Measure Count                    |
| `count_of_facility_safety_measures`          | STRING | No          | Count of Facility Safety Measures             |
| `count_of_safety_measures_better`            | STRING | No          | Count of Safety Measures Better               |
| `count_of_safety_measures_no_different`      | STRING | No          | Count of Safety Measures No Different         |
| `count_of_safety_measures_worse`             | STRING | No          | Count of Safety Measures Worse                |
| `safety_group_footnote`                      | STRING | No          | Safety Group Footnote                         |
| `readm_group_measure_count`                  | STRING | No          | Readmission Group Measure Count               |
| `count_of_facility_readm_measures`           | STRING | No          | Count of Facility Readmission Measures        |
| `count_of_readm_measures_better`             | STRING | No          | Count of Readmission Measures Better          |
| `count_of_readm_measures_no_different`       | STRING | No          | Count of Readmission Measures No Different    |
| `count_of_readm_measures_worse`              | STRING | No          | Count of Readmission Measures Worse           |
| `readm_group_footnote`                       | STRING | No          | Readmission Group Footnote                    |
| `pt_exp_group_measure_count`                 | STRING | No          | Patient Experience Group Measure Count        |
| `count_of_facility_pt_exp_measures`          | STRING | No          | Count of Facility Patient Experience Measures |
| `pt_exp_group_footnote`                      | STRING | No          | Patient Experience Group Footnote             |
| `te_group_measure_count`                     | STRING | No          | Timely & Effective Care Group Measure Count   |
| `count_of_facility_te_measures`              | STRING | No          | Count of Facility Timely & Effective Measures |
| `te_group_footnote`                          | STRING | No          | Timely & Effective Care Group Footnote        |

## Setup and Installation

To set up and run this Fivetran connector locally for development and testing:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Darshpreet2000/healthcostcompare-connector.git
    cd FiveTranConnector
    ```
2.  **Install dependencies:**
    Ensure you have Python installed. Then install the required packages:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configure:**
    Create a `configuration.json` file in the root directory if it doesn't exist. For local debugging, it can be an empty JSON object `{}`.
4.  **Run the connector:**
    ```bash
    python connector.py
    ```

## Usage

This connector is designed to be deployed and managed by Fivetran. Once configured within the Fivetran platform, it will automatically synchronize data from the specified CMS APIs into your destination data warehouse according to the defined schemas.

## Technologies Used

*   **Language:** Python
*   **SDK:** Fivetran Connector SDK
*   **HTTP Requests:** `requests` library

## Contributing

Contributions are welcome! Please refer to the contribution guidelines (if available) for more information.
