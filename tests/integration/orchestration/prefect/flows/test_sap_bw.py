"""'test_sap_bw.py'."""

from datetime import date, timedelta

from viadot.orchestration.prefect.flows import sap_bw_to_adls


present_day = date.today()
present_day_str = present_day.strftime("%Y%m%d")
past_day = date.today() - timedelta(7)
past_day_str = past_day.strftime("%Y%m%d")
mdx_query = f"""
    SELECT
        {{[Measures].[003YPR44RQTVKWX9OL316XK7J],
        [Measures].[003YPR44RQTVKWX9T5BFEWUY9],
        [Measures].[003YPR44RQTVKWX9UW92X7ELV],
        [Measures].[003YPR44RQTVKWX9YLKYZKH5O],
        [Measures].[003YPR44RQTVKWXA51D4J8HRZ],
        [Measures].[003YPR44RQTVKWXAEL7KFSJ6U],
        [Measures].[003YPR44RQTVKWXARA4PVZ9TK]}}
    ON COLUMNS,
    NON EMPTY
     {{ [0BILLTOPRTY].[LEVEL01].MEMBERS *
        [0CALMONTH__0CALMONTH2].[LEVEL01].MEMBERS *
        [0COMP_CODE].[LEVEL01].MEMBERS *
        [0COMP_CODE__ZCOMPCOTE].[LEVEL01].MEMBERS *
        [0CREATEDBY].[LEVEL01].MEMBERS *
        [0CREATEDON].[LEVEL01].MEMBERS *
        [0DISTR_CHAN].[LEVEL01].MEMBERS *
        [0DOC_NUMBER].[LEVEL01].MEMBERS *
        [0DOC_TYPE].[LEVEL01].MEMBERS *
        [0IMODOCCAT].[LEVEL01].MEMBERS *
        [0MATERIAL__ZDSPPRIC].[LEVEL01].MEMBERS *
        [0MATERIAL__ZPRDGRP].[LEVEL01].MEMBERS *
        [0MATERIAL__ZPANEVACD].[LEVEL01].MEMBERS *
        [0MATERIAL__ZPRODAREA].[LEVEL01].MEMBERS *
        [0MATERIAL__ZRDBTYPE].[LEVEL01].MEMBERS *
        [0MATERIAL__ZTYPEVAR].[LEVEL01].MEMBERS *
        [0MATERIAL__ZVAR_C34].[LEVEL01].MEMBERS *
        [0MATERIAL__ZVAR_CH1].[LEVEL01].MEMBERS *
        [0MATERIAL__ZVAR_CH2].[LEVEL01].MEMBERS *
        [0MATERIAL__ZVCIVAR2].[LEVEL01].MEMBERS *
        [0ORD_REASON].[LEVEL01].MEMBERS *
        [0PAYER].[LEVEL01].MEMBERS *
        [0REASON_REJ].[LEVEL01].MEMBERS *
        [0SALESORG].[LEVEL01].MEMBERS *
        [0SHIP_TO].[LEVEL01].MEMBERS *
        [0SOLD_TO].[LEVEL01].MEMBERS *
        [0SOLD_TO__0ACCNT_GRP].[LEVEL01].MEMBERS *
        [0USAGE_IND].[LEVEL01].MEMBERS *
        [0USAGE_IND__ZSALECAT].[LEVEL01].MEMBERS *
        [ZASE_ID].[LEVEL01].MEMBERS *
        [ZBVTSPRST].[LEVEL01].MEMBERS *
        [ZCALWEEK].[LEVEL01].MEMBERS *
        [ZORD_CREA].[LEVEL01].MEMBERS *
        [ZPONUMBER].[LEVEL01].MEMBERS *
        [ZPUORTYPE].[LEVEL01].MEMBERS *
        [ZSEG_HDR].[LEVEL01].MEMBERS *
        [0SALESEMPLY].[LEVEL01].MEMBERS *
        [0SHIP_TO__0ACCNT_GRP].[LEVEL01].MEMBERS *
        [0SHIP_TO__0CITY].[LEVEL01].MEMBERS *
        [0CALYEAR].[LEVEL01].MEMBERS *
        [0CALMONTH].[LEVEL01].MEMBERS *
        {{[0CALDAY].[{past_day_str}] : [0CALDAY].[{present_day_str}]}}}}
    DIMENSION PROPERTIES
    MEMBER_NAME,
    MEMBER_CAPTION
    ON ROWS
    FROM ZCSALORD1/ZBW4_ZCSALORD1_002_BOA
"""

mapping_dict = {
    "[0BILLTOPRTY].[LEVEL01].[MEMBER_NAME]": "bill_to_party",
    "[0BILLTOPRTY].[LEVEL01].[MEMBER_CAPTION]": "bill_to_party_id",
    "[0CALMONTH__0CALMONTH2].[LEVEL01].[MEMBER_CAPTION]": "calendar_month_2",
    "[0CALMONTH__0CALMONTH2].[LEVEL01].[MEMBER_NAME]": "calendar_month_id",
    "[0COMP_CODE].[LEVEL01].[MEMBER_CAPTION]": "company_code",
    "[0COMP_CODE].[LEVEL01].[MEMBER_NAME]": "company_code_name",
    "[0COMP_CODE__ZCOMPCOTE].[LEVEL01].[MEMBER_CAPTION]": "company_code_cons_term",
    "[0COMP_CODE__ZCOMPCOTE].[LEVEL01].[MEMBER_NAME]": "company_code_cons_term_name",
    "[0CREATEDBY].[LEVEL01].[MEMBER_CAPTION]": "created_by",
    "[0CREATEDBY].[LEVEL01].[MEMBER_NAME]": "created_by_name",
    "[0CREATEDON].[LEVEL01].[MEMBER_CAPTION]": "created_on",
    "[0CREATEDON].[LEVEL01].[MEMBER_NAME]": "created_on_name",
    "[0DISTR_CHAN].[LEVEL01].[MEMBER_CAPTION]": "distribution_channel",
    "[0DISTR_CHAN].[LEVEL01].[MEMBER_NAME]": "distribution_channel_name",
    "[0DOC_NUMBER].[LEVEL01].[MEMBER_CAPTION]": "sales_document",
    "[0DOC_NUMBER].[LEVEL01].[MEMBER_NAME]": "sales_document_name",
    "[0DOC_TYPE].[LEVEL01].[MEMBER_CAPTION]": "sales_doc_type",
    "[0DOC_TYPE].[LEVEL01].[MEMBER_NAME]": "sales_doc_type_name",
    "[0IMODOCCAT].[LEVEL01].[MEMBER_CAPTION]": "sales_document_categ",
    "[0IMODOCCAT].[LEVEL01].[MEMBER_NAME]": "sales_document_categ_name",
    "[0MATERIAL__ZDSPPRIC].[LEVEL01].[MEMBER_CAPTION]": "dsp_pricing_group",
    "[0MATERIAL__ZDSPPRIC].[LEVEL01].[MEMBER_NAME]": "dsp_pricing_group_name",
    "[0MATERIAL__ZPANEVACD].[LEVEL01].[MEMBER_CAPTION]": "pane_variant_code",
    "[0MATERIAL__ZPANEVACD].[LEVEL01].[MEMBER_NAME]": "pane_variant_code_name",
    "[0MATERIAL__ZPRDGRP].[LEVEL01].[MEMBER_CAPTION]": "product_group",
    "[0MATERIAL__ZPRDGRP].[LEVEL01].[MEMBER_NAME]": "product_group_name",
    "[0MATERIAL__ZPRODAREA].[LEVEL01].[MEMBER_CAPTION]": "product_area",
    "[0MATERIAL__ZPRODAREA].[LEVEL01].[MEMBER_NAME]": "product_area_name",
    "[0MATERIAL__ZRDBTYPE].[LEVEL01].[MEMBER_CAPTION]": "type_of_material",
    "[0MATERIAL__ZRDBTYPE].[LEVEL01].[MEMBER_NAME]": "type_of_material_name",
    "[0MATERIAL__ZTYPEVAR].[LEVEL01].[MEMBER_CAPTION]": "material_type_variant",
    "[0MATERIAL__ZTYPEVAR].[LEVEL01].[MEMBER_NAME]": "material_type_variant_name",
    "[0MATERIAL__ZVAR_C34].[LEVEL01].[MEMBER_CAPTION]": "3_and_4_character",
    "[0MATERIAL__ZVAR_C34].[LEVEL01].[MEMBER_NAME]": "3_and_4_character_name",
    "[0MATERIAL__ZVAR_CH1].[LEVEL01].[MEMBER_CAPTION]": "1_character_of_vari",
    "[0MATERIAL__ZVAR_CH1].[LEVEL01].[MEMBER_NAME]": "1_character_of_vari_name",
    "[0MATERIAL__ZVAR_CH2].[LEVEL01].[MEMBER_CAPTION]": "2_character_of_vari",
    "[0MATERIAL__ZVAR_CH2].[LEVEL01].[MEMBER_NAME]": "2_character_of_vari_name",
    "[0MATERIAL__ZVCIVAR2].[LEVEL01].[MEMBER_CAPTION]": "product_variant",
    "[0MATERIAL__ZVCIVAR2].[LEVEL01].[MEMBER_NAME]": "product_variant_name",
    "[0ORD_REASON].[LEVEL01].[MEMBER_CAPTION]": "reason_for_order",
    "[0ORD_REASON].[LEVEL01].[MEMBER_NAME]": "reason_for_order_name",
    "[0PAYER].[LEVEL01].[MEMBER_CAPTION]": "payer",
    "[0PAYER].[LEVEL01].[MEMBER_NAME]": "payer_name",
    "[0REASON_REJ].[LEVEL01].[MEMBER_CAPTION]": "reason_for_rejection",
    "[0REASON_REJ].[LEVEL01].[MEMBER_NAME]": "reason_for_rejection_name",
    "[0SALESORG].[LEVEL01].[MEMBER_CAPTION]": "sales_organization",
    "[0SALESORG].[LEVEL01].[MEMBER_NAME]": "sales_organization_name",
    "[0SHIP_TO].[LEVEL01].[MEMBER_CAPTION]": "ship_to_party",
    "[0SHIP_TO].[LEVEL01].[MEMBER_NAME]": "ship_to_party_name",
    "[0SOLD_TO].[LEVEL01].[MEMBER_CAPTION]": "sold_to_party",
    "[0SOLD_TO].[LEVEL01].[MEMBER_NAME]": "sold_to_party_name",
    "[0SOLD_TO__0ACCNT_GRP].[LEVEL01].[MEMBER_CAPTION]": "customer_account_group_sold_to",
    "[0SOLD_TO__0ACCNT_GRP].[LEVEL01].[MEMBER_NAME]": "customer_account_group_sold_to_name",
    "[0USAGE_IND].[LEVEL01].[MEMBER_CAPTION]": "usage_indicator",
    "[0USAGE_IND].[LEVEL01].[MEMBER_NAME]": "usage_indicator_name",
    "[0USAGE_IND__ZSALECAT].[LEVEL01].[MEMBER_CAPTION]": "sales_cat_usage",
    "[0USAGE_IND__ZSALECAT].[LEVEL01].[MEMBER_NAME]": "sales_cat_usage_name",
    "[ZASE_ID].[LEVEL01].[MEMBER_CAPTION]": "ase_id",
    "[ZASE_ID].[LEVEL01].[MEMBER_NAME]": "ase_id_name",
    "[ZBVTSPRST].[LEVEL01].[MEMBER_CAPTION]": "order_status",
    "[ZBVTSPRST].[LEVEL01].[MEMBER_NAME]": "order_status_name",
    "[ZCALWEEK].[LEVEL01].[MEMBER_CAPTION]": "calendar_week",
    "[ZCALWEEK].[LEVEL01].[MEMBER_NAME]": "calendar_week_name",
    "[ZORD_CREA].[LEVEL01].[MEMBER_CAPTION]": "order_creation_date",
    "[ZORD_CREA].[LEVEL01].[MEMBER_NAME]": "order_creation_date_name",
    "[ZPONUMBER].[LEVEL01].[MEMBER_CAPTION]": "po_number",
    "[ZPONUMBER].[LEVEL01].[MEMBER_NAME]": "po_number_name",
    "[ZPUORTYPE].[LEVEL01].[MEMBER_CAPTION]": "purchase_order_type",
    "[ZPUORTYPE].[LEVEL01].[MEMBER_NAME]": "purchase_order_type_name",
    "[ZSEG_HDR].[LEVEL01].[MEMBER_CAPTION]": "segment_header",
    "[ZSEG_HDR].[LEVEL01].[MEMBER_NAME]": "segment_header_name",
    "[0SALESEMPLY].[LEVEL01].[MEMBER_CAPTION]": "sales_representative",
    "[0SALESEMPLY].[LEVEL01].[MEMBER_NAME]": "sales_representative_name",
    "[0SHIP_TO__0ACCNT_GRP].[LEVEL01].[MEMBER_CAPTION]": "customer_account_group_ship_to",
    "[0SHIP_TO__0ACCNT_GRP].[LEVEL01].[MEMBER_NAME]": "customer_account_group_ship_to_name",
    "[0SHIP_TO__0CITY].[LEVEL01].[MEMBER_CAPTION]": "location_ship_to",
    "[0SHIP_TO__0CITY].[LEVEL01].[MEMBER_NAME]": "location_ship_to_name",
    "[0CALDAY].[LEVEL01].[MEMBER_CAPTION]": "calendar_day",
    "[0CALDAY].[LEVEL01].[MEMBER_NAME]": "calendar_day_name",
    "[0CALMONTH].[LEVEL01].[MEMBER_CAPTION]": "calendar_month",
    "[0CALMONTH].[LEVEL01].[MEMBER_NAME]": "calendar_month_name",
    "[0CALYEAR].[LEVEL01].[MEMBER_CAPTION]": "calendar_year",
    "[0CALYEAR].[LEVEL01].[MEMBER_NAME]": "calendar_year_name",
    "[Measures].[003YPR44RQTVKWX9OL316XK7J]": "net_value",
    "[Measures].[003YPR44RQTVKWX9T5BFEWUY9]": "order_quantity",
    "[Measures].[003YPR44RQTVKWX9UW92X7ELV]": "open_orders_quantity",
    "[Measures].[003YPR44RQTVKWX9YLKYZKH5O]": "number_of_sales_orders",
    "[Measures].[003YPR44RQTVKWXA51D4J8HRZ]": "number_of_quotations",
    "[Measures].[003YPR44RQTVKWXAEL7KFSJ6U]": "number_of_orders_created_from_quotations",
    "[Measures].[003YPR44RQTVKWXARA4PVZ9TK]": "number_of_quotations_expired_validity_date",
}


def test_sap_bw_to_adls(sap_bw_config_key, adls_credentials_secret):
    state = sap_bw_to_adls(
        azure_key_vault_secret=sap_bw_config_key,
        mdx_query=mdx_query,
        mapping_dict=mapping_dict,
        adls_path="raw/dyvenia_sandbox/sap_bw/sab_bw.parquet",
        adls_azure_key_vault_secret=adls_credentials_secret,
        adls_path_overwrite=True,
    )
    assert state.is_successful()
