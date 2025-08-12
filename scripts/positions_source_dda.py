# Import Packages
from google.cloud import bigquery


# Initialize variables

PROJECT_ID = 'cidat-10040-int-445d'
DATASET_ID = 'ez_cb_positions_data_interactive'
TABLE_NAME = 'positions_source_dda'


query = """
    SELECT
        CAST(d.business_effective_date AS timestamp) AS effective_d,
        d.source_system_name AS source_s,
        LOWER(d.currency_cd) AS currency,
        null AS notional_ccy,
        null AS mtm_ccy,
        d.business_segment_lookup_cd AS source_book,
        d.position_id AS trade_id,
        d.position_id AS account,
        d.customer_lookup_cd AS counterparty,
        d.interest_rate AS interest_rate,
        CASE
            WHEN d.account_status_type_name != 'Closed'
                THEN d.position_id
            END AS active_account,
        CASE
            WHEN d.financial_record_type_name = 'Off Balance Sheet' 
                THEN 'Off BS'
            WHEN d.financial_record_type_name = 'Off Balance Sheet - Business Development Bank of Canada (BDC)' 
                THEN 'Off BS - BDC'
            WHEN d.financial_record_type_name = 'Off Balance Sheet - Canada Emergency Business Account (CEBA)' 
                THEN 'Off BS - CEBA'
            WHEN d.financial_record_type_name = 'On Balance Sheet' 
                THEN 'On BS'
            ELSE d.financial_record_type_name
            END AS bs_record_type,
        d.asset_gross_cad_amt AS asset_gross_cad,
        d.liability_gross_cad_amt AS liability_gross_cad,
        d.asset_gross_coa_amt AS asset_gross_coa,
        d.liability_gross_coa_amt AS liability_gross_coa,
        d.asset_gross_usd_amt AS asset_gross_usd,
        d.liability_gross_usd_amt AS liability_gross_usd,
        0 AS notional_cad,
        0 AS mtm_cad,
        d.product_reference_source_name AS product_namespace,
        d.business_segment_reference_source_name AS book_namespace,
        d.customer_reference_source_name AS counterparty_namespace,
        d.sic_cd AS sic,
        CAST(d.maturity_date AS timestamp) AS maturity_dt,
        null AS product_registered_type,
        d.interest_base_rate_type_name AS reporting_rate_type_desc,
        CONCAT(d.source_system_name, '-', d.position_id) AS account_key,
        dense_rank() over (order by d.business_effective_date desc) desc_day_index,
        CAST(null AS date) AS origination_date,
        d.maturity_value_cad_amt AS maturity_value,
        d.authorized_overdraft_limit_cad_amt AS authorized_loan_limit,
        
        d.account_open_date, 
        d.start_date,

		'' as remaining_term_bucket_name,
		0 as acct_cad_opr_limit_cad_amt,
		0 as acct_usd_opr_limit_usd_amt,
		0 as acct_usd_opr_limit_cad_amt,
		0 AS authorized_loan_r_limit_cad_amt,
		0 AS authorized_loan_nr_limit_cad_amt,

        b.business_level_1 AS bl1,
        b.business_level_2 AS bl2,
        b.business_level_3 AS bl3,
        b.business_level_4 AS bl4,
        b.business_level_5 AS bl5,
        b.business_level_6 AS bl6,
        b.book_consolidation AS book_text,
        CAST(b.transit AS integer) AS transit,

        c.legal_name AS client,
        c.parent_name AS parent,
        c.ultimate_parent_name AS ultimate_name,
        c.customer_type_report AS customer_type_report,
        CONCAT(c.source_system_name, "-", LTRIM(c.source_business_customer_id, '0')) AS client_key,
        '' AS client_id,
        LTRIM(c.source_business_customer_id, '0') AS cis_id,
        '' AS fenergo_id,
        CAST(customer_effective_date AS timestamp) AS customer_since_date,

        cal.canadaholidayflag,
        cal.monthendflag,
        cal.quarterendflag,
        cal.yearendflag,

        p.source_product_code AS source_product,
        p.source_product_type AS source_ftp_product_type,
        p.enterprise_product_name AS product,
        p.level1 AS level1,
        p.level2 AS level2,
        p.level3 AS level3,
        p.level4 AS level4,
        p.level5 AS level5


    FROM
        `cidat-10028-prd-dda9.ez_cb_positions_data.ent_positions_dda` d

        LEFT OUTER JOIN
            `cidat-10040-int-445d.client360_interactive.positions_ref_book_source` b
            ON
                d.business_segment_reference_source_name = b.source AND
                d.business_segment_lookup_cd = b.book_string_sourcesystem

        LEFT OUTER JOIN
            (
            SELECT * FROM cidat-10028-prd-dda9.ez_cb_customers_data.ent_consolidated_customer_history_individual_cis WHERE business_effective_date BETWEEN date_sub(current_date, INTERVAL 750 DAY) AND current_date 
            UNION ALL 
            SELECT * FROM cidat-10028-prd-dda9.ez_cb_customers_data.ent_consolidated_customer_history_institution_cis WHERE business_effective_date BETWEEN date_sub(current_date, INTERVAL 750 DAY) AND current_date 
            ) c
            ON
                d.customer_reference_source_name = c.source_system_name AND
                d.customer_lookup_cd = c.source_business_customer_id AND
                d.business_effective_date = c.business_effective_date

        INNER JOIN
            `cidat-10040-int-445d.client360_interactive.positions_ref_calendar` cal
            ON
                d.business_effective_date = cal.ddate

        LEFT OUTER JOIN
            `cidat-10029-prd-9414.cz_rdm_data_view.view_ex1_mpp_lkp_product_hry_flatten` p
            ON
                d.product_reference_source_name = p.source AND
                d.product_lookup_cd = p.source_product_code

        LEFT OUTER JOIN
            (SELECT * 
                FROM `cidat-10029-prd-9414.cz_rdm_data_view.view_cm1_industry_classification_hierarchy_flatten`
                WHERE hierarchy_category_cd in ('SIC-E 1980')) s
            ON
                d.sic_cd = s.industry_classification_hierarchy_cd



    WHERE
        d.business_effective_date BETWEEN date_sub(current_date, INTERVAL 750 DAY) AND current_date
        AND COALESCE(b.business_level_2,'Retail') != 'Retail'
        AND (b.business_level_2 != 'Commercial' or (b.business_level_2 = 'Commercial' and c.customer_type_report = 'Commercial'))
    """


# Construct BigQuery objects
client = bigquery.Client()
table_id = '{}.{}.{}'.format(PROJECT_ID, DATASET_ID, TABLE_NAME)


# Upload Dataframe data to BQ Table
job_config = bigquery.QueryJobConfig(write_disposition="WRITE_TRUNCATE",destination=table_id)

job = client.query(query, job_config=job_config)
job.result()  # Wait for the job to complete.

print("Query Uploaded")


table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)