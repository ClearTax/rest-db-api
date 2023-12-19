from rest_db_api import rest_api_adapter, utils


class TestQueryParsing:
    @staticmethod
    def test_parse_operation_and_uri_1():
        uri_1 = "/reports/v2.0/ledgers"
        operation_1 = """SELECT * from "/reports/v2.0/ledgers"
        where QUERY_PARAMcount = 100
              and "HEADERx-clear-node-id" in ("test-header-6", "test-header-9")
        LIMIT 1001"""

        uri_response_1, operation_response_1 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_1, operation_1)
        assert uri_response_1 == "/reports/v2.0/ledgers?count=100&header=x-clear-node-id:test-header-6&header=x-clear-node-id:test-header-9"
        assert operation_response_1 == 'SELECT * FROM "/reports/v2.0/ledgers?count=100&header=x-clear-node-id:test-header-6&header=x-clear-node-id:test-header-9" LIMIT 1001'

        uri_response_2, operation_response_2 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_response_1,
                                                                                                    operation_response_1)
        assert uri_response_2 == uri_response_1
        assert operation_response_2 == operation_response_1

        path, query_params, headers_dict, fragment, body = rest_api_adapter.RestAdapter.parse_uri(uri_response_2)
        assert path == "/reports/v2.0/ledgers"
        assert query_params == {"count": ["100"], }
        assert headers_dict == {"x-clear-node-id": "test-header-6,test-header-9", }
        assert fragment == '$[*]'
        assert body == {}

    @staticmethod
    def test_parse_operation_and_uri_2():
        uri_1 = "/reports/v2.0/ledgers"
        operation_1 = """SELECT * from "/reports/v2.0/ledgers"
        where colour = blue and QUERY_PARAMcount = 100 and QUERY_PARAMpage in (5,6)
              and "HEADERx-clear-node-id" in ("test-header-6", "test-header-9") and first_name = "big_mack" and "HEADERx-clear-node-type" = "node-type"
              and dress_code = pink
              order BY test_order_column desc 
        LIMIT 1001"""

        uri_response_1, operation_response_1 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_1, operation_1)
        assert uri_response_1 == "/reports/v2.0/ledgers?count=100&page=5&page=6&header=x-clear-node-id:test-header-6&header=x-clear-node-id:test-header-9&header=x-clear-node-type:node-type"
        assert operation_response_1 == 'SELECT * FROM "/reports/v2.0/ledgers?count=100&page=5&page=6&header=x-clear-node-id:test-header-6&header=x-clear-node-id:test-header-9&header=x-clear-node-type:node-type" WHERE colour = blue AND first_name = "big_mack" AND dress_code = pink ORDER BY test_order_column DESC LIMIT 1001'

        uri_response_2, operation_response_2 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_response_1,
                                                                                                    operation_response_1)
        assert uri_response_2 == uri_response_1
        assert operation_response_2 == operation_response_1

        path, query_params, headers_dict, fragment, body = rest_api_adapter.RestAdapter.parse_uri(uri_response_2)
        assert path == "/reports/v2.0/ledgers"
        assert query_params == {"count": ["100"], "page": ["5", "6"]}
        assert headers_dict == {"x-clear-node-id": "test-header-6,test-header-9", "x-clear-node-type": "node-type"}
        assert fragment == '$[*]'
        assert body == {}

    @staticmethod
    def test_parse_operation_and_uri_3():
        uri_1 = "/api/gst-reports/reports/v3.0/ledgers/transaction"
        operation_1 = """SELECT gstin AS gstin,
           "gstinNodeId" AS "gstinNodeId",
           date AS date,
           "referenceNo" AS "referenceNo",
           description AS description,
           table_4_a_5_igst AS table_4_a_5_igst,
           table_4_a_5_cgst AS table_4_a_5_cgst,
           table_4_a_5_sgst AS table_4_a_5_sgst,
           table_4_a_5_cess AS table_4_a_5_cess,
           table_4_b_2_igst AS table_4_b_2_igst,
           table_4_b_2_cgst AS table_4_b_2_cgst,
           table_4_b_2_sgst AS table_4_b_2_sgst,
           table_4_b_2_cess AS table_4_b_2_cess,
           table_4_d_1_igst AS table_4_d_1_igst,
           table_4_d_1_cgst AS table_4_d_1_cgst,
           table_4_d_1_sgst AS table_4_d_1_sgst,
           table_4_d_1_cess AS table_4_d_1_cess,
           table_closing_igst AS table_closing_igst,
           table_closing_cgst AS table_closing_cgst,
           table_closing_sgst AS table_closing_sgst,
           table_closing_cess AS table_closing_cess,
           rowid AS rowid
    FROM
      (SELECT *
       FROM "/api/gst-reports/reports/v3.0/ledgers/transaction"
       WHERE "QUERY_PARAMjob_id"="7227c292-2812-472f-bee3-9333195de314"
         AND "HEADERx-job-type"="PAN_ELECTRONIC_REVERSAL_LEDGER"
         AND "HEADERx-clear-node-id"="5614e031-4bc7-4ebb-b90e-a3d4230223fc,5e3fa661-2f00-4fcb-98ee-6739406bdfad"
         AND "HEADERx-clear-node-type"="GSTIN"
         AND "HEADERx-workspace-id"="ef004585-7344-409f-89d0-f39721dec8b9"
         AND "HEADERx-cleartax-orgunit"="e1378c1f-35f5-494b-9903-fd851f238613"
         AND "HEADERcookie"="ctLangPreference=eng_IND; sid=1.7764ff10-b51d-439d-9fff-442eb4aee51f_3b7b9f9e10822dc6277f1435ac445f9dbfa9289465390c9346591cefc0fe49ce; ssoAId=f4813ac4-75ae-4205-89cb-390cb44cdbdd"
         AND "HEADERx-cleartax-product"="GST") AS virtual_table
    LIMIT 1000
    OFFSET 0;"""

        uri_response_1, operation_response_1 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_1, operation_1)
        assert uri_response_1 == '/api/gst-reports/reports/v3.0/ledgers/transaction?job_id=7227c292-2812-472f-bee3-9333195de314&header=x-job-type:PAN_ELECTRONIC_REVERSAL_LEDGER&header=x-clear-node-id:5614e031-4bc7-4ebb-b90e-a3d4230223fc,5e3fa661-2f00-4fcb-98ee-6739406bdfad&header=x-clear-node-type:GSTIN&header=x-workspace-id:ef004585-7344-409f-89d0-f39721dec8b9&header=x-cleartax-orgunit:e1378c1f-35f5-494b-9903-fd851f238613&header=cookie:ctLangPreference=eng_IND; sid=1.7764ff10-b51d-439d-9fff-442eb4aee51f_3b7b9f9e10822dc6277f1435ac445f9dbfa9289465390c9346591cefc0fe49ce; ssoAId=f4813ac4-75ae-4205-89cb-390cb44cdbdd&header=x-cleartax-product:GST'
        assert operation_response_1 == 'SELECT gstin AS gstin, "gstinNodeId" AS "gstinNodeId", date AS date, "referenceNo" AS "referenceNo", description AS description, table_4_a_5_igst AS table_4_a_5_igst, table_4_a_5_cgst AS table_4_a_5_cgst, table_4_a_5_sgst AS table_4_a_5_sgst, table_4_a_5_cess AS table_4_a_5_cess, table_4_b_2_igst AS table_4_b_2_igst, table_4_b_2_cgst AS table_4_b_2_cgst, table_4_b_2_sgst AS table_4_b_2_sgst, table_4_b_2_cess AS table_4_b_2_cess, table_4_d_1_igst AS table_4_d_1_igst, table_4_d_1_cgst AS table_4_d_1_cgst, table_4_d_1_sgst AS table_4_d_1_sgst, table_4_d_1_cess AS table_4_d_1_cess, table_closing_igst AS table_closing_igst, table_closing_cgst AS table_closing_cgst, table_closing_sgst AS table_closing_sgst, table_closing_cess AS table_closing_cess, rowid AS rowid FROM (SELECT * FROM "/api/gst-reports/reports/v3.0/ledgers/transaction?job_id=7227c292-2812-472f-bee3-9333195de314&header=x-job-type:PAN_ELECTRONIC_REVERSAL_LEDGER&header=x-clear-node-id:5614e031-4bc7-4ebb-b90e-a3d4230223fc,5e3fa661-2f00-4fcb-98ee-6739406bdfad&header=x-clear-node-type:GSTIN&header=x-workspace-id:ef004585-7344-409f-89d0-f39721dec8b9&header=x-cleartax-orgunit:e1378c1f-35f5-494b-9903-fd851f238613&header=cookie:ctLangPreference=eng_IND; sid=1.7764ff10-b51d-439d-9fff-442eb4aee51f_3b7b9f9e10822dc6277f1435ac445f9dbfa9289465390c9346591cefc0fe49ce; ssoAId=f4813ac4-75ae-4205-89cb-390cb44cdbdd&header=x-cleartax-product:GST") AS virtual_table LIMIT 1000 OFFSET 0'

        uri_response_2, operation_response_2 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_response_1,
                                                                                                    operation_response_1)
        assert uri_response_2 == uri_response_1
        assert operation_response_2 == operation_response_1

        path, query_params, headers_dict, fragment, body = rest_api_adapter.RestAdapter.parse_uri(uri_response_2)
        assert path == "/api/gst-reports/reports/v3.0/ledgers/transaction"
        assert query_params == {'job_id': ['7227c292-2812-472f-bee3-9333195de314']}
        assert headers_dict == {'x-job-type': 'PAN_ELECTRONIC_REVERSAL_LEDGER',
                                'x-clear-node-id': '5614e031-4bc7-4ebb-b90e-a3d4230223fc,5e3fa661-2f00-4fcb-98ee-6739406bdfad',
                                'x-clear-node-type': 'GSTIN', 'x-workspace-id': 'ef004585-7344-409f-89d0-f39721dec8b9',
                                'x-cleartax-orgunit': 'e1378c1f-35f5-494b-9903-fd851f238613',
                                'cookie': 'ctLangPreference=eng_IND; sid=1.7764ff10-b51d-439d-9fff-442eb4aee51f_3b7b9f9e10822dc6277f1435ac445f9dbfa9289465390c9346591cefc0fe49ce; ssoAId=f4813ac4-75ae-4205-89cb-390cb44cdbdd',
                                'x-cleartax-product': 'GST'}
        assert fragment == '$[*]'
        assert body == {}

    @staticmethod
    def test_parse_operation_and_uri_4():
        uri_1 = "REPORTS_LEDGER_SUMMARY"
        operation_1 = """SELECT gstin AS gstin,
           "gstinNodeId" AS "gstinNodeId",
           date AS date,
           "referenceNo" AS "referenceNo",
           description AS description,
           table_4_a_5_igst AS table_4_a_5_igst,
           table_4_a_5_cgst AS table_4_a_5_cgst,
           table_4_a_5_sgst AS table_4_a_5_sgst,
           table_4_a_5_cess AS table_4_a_5_cess,
           table_4_b_2_igst AS table_4_b_2_igst,
           table_4_b_2_cgst AS table_4_b_2_cgst,
           table_4_b_2_sgst AS table_4_b_2_sgst,
           table_4_b_2_cess AS table_4_b_2_cess,
           table_4_d_1_igst AS table_4_d_1_igst,
           table_4_d_1_cgst AS table_4_d_1_cgst,
           table_4_d_1_sgst AS table_4_d_1_sgst,
           table_4_d_1_cess AS table_4_d_1_cess,
           table_closing_igst AS table_closing_igst,
           table_closing_cgst AS table_closing_cgst,
           table_closing_sgst AS table_closing_sgst,
           table_closing_cess AS table_closing_cess,
           rowid AS rowid
    FROM
      (SELECT *
       FROM "REPORTS_LEDGER_SUMMARY"
       WHERE "QUERY_PARAMjob_id"="7227c292-2812-472f-bee3-9333195de314") AS virtual_table
    
    WHere (org_id in ("e1378c1f-35f5-494b-9903-fd851f238613")
            and node_ids in ("5614e031-4bc7-4ebb-b90e-a3d4230223fc", "5e3fa661-2f00-4fcb-98ee-6739406bdfad"))
    LIMIT 1000
    OFFSET 0;"""

        uri_response_1, operation_response_1 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_1, operation_1)
        assert uri_response_1 == 'REPORTS_LEDGER_SUMMARY?job_id=7227c292-2812-472f-bee3-9333195de314&header=x-cleartax-orgunit:e1378c1f-35f5-494b-9903-fd851f238613&header=x-clear-node-id:5614e031-4bc7-4ebb-b90e-a3d4230223fc&header=x-clear-node-id:5e3fa661-2f00-4fcb-98ee-6739406bdfad'
        assert operation_response_1 == 'SELECT gstin AS gstin, "gstinNodeId" AS "gstinNodeId", date AS date, "referenceNo" AS "referenceNo", description AS description, table_4_a_5_igst AS table_4_a_5_igst, table_4_a_5_cgst AS table_4_a_5_cgst, table_4_a_5_sgst AS table_4_a_5_sgst, table_4_a_5_cess AS table_4_a_5_cess, table_4_b_2_igst AS table_4_b_2_igst, table_4_b_2_cgst AS table_4_b_2_cgst, table_4_b_2_sgst AS table_4_b_2_sgst, table_4_b_2_cess AS table_4_b_2_cess, table_4_d_1_igst AS table_4_d_1_igst, table_4_d_1_cgst AS table_4_d_1_cgst, table_4_d_1_sgst AS table_4_d_1_sgst, table_4_d_1_cess AS table_4_d_1_cess, table_closing_igst AS table_closing_igst, table_closing_cgst AS table_closing_cgst, table_closing_sgst AS table_closing_sgst, table_closing_cess AS table_closing_cess, rowid AS rowid FROM (SELECT * FROM "REPORTS_LEDGER_SUMMARY?job_id=7227c292-2812-472f-bee3-9333195de314&header=x-cleartax-orgunit:e1378c1f-35f5-494b-9903-fd851f238613&header=x-clear-node-id:5614e031-4bc7-4ebb-b90e-a3d4230223fc&header=x-clear-node-id:5e3fa661-2f00-4fcb-98ee-6739406bdfad") AS virtual_table LIMIT 1000 OFFSET 0'

        uri_response_2, operation_response_2 = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri_response_1,
                                                                                                    operation_response_1)
        assert uri_response_2 == uri_response_1
        assert operation_response_2 == operation_response_1

        path, query_params, headers_dict, fragment, body = rest_api_adapter.RestAdapter.parse_uri(uri_response_2)
        assert path == "REPORTS_LEDGER_SUMMARY"
        assert query_params == {
            'job_id': [
                '7227c292-2812-472f-bee3-9333195de314'
            ]
        }
        assert headers_dict == {
            'x-clear-node-id': '5614e031-4bc7-4ebb-b90e-a3d4230223fc,5e3fa661-2f00-4fcb-98ee-6739406bdfad',
            'x-cleartax-orgunit': 'e1378c1f-35f5-494b-9903-fd851f238613',
        }
        assert fragment == '$[*]'
        assert body == {}

    @staticmethod
    def test_remove_redundant_clause():
        assert "WHERE (org_id = 1234)" == utils.remove_invalid_clause("WHERE (org_id = 1234)")
        assert "" == utils.remove_invalid_clause("WHERE ()")
        assert "" == utils.remove_invalid_clause("WHERE ( )")
        assert "" == utils.remove_invalid_clause("WHERE (    )")
        assert "" == utils.remove_invalid_clause("AND ( )")
        assert "" == utils.remove_invalid_clause("AND ()")
        assert "" == utils.remove_invalid_clause("WHERE ( AND () )")

    @staticmethod
    def test_invalid_operation():
        uri = "/api/gst-reports/reports/v3.0/ledgers/transaction"
        invalid_operation = "SELECT * FROM WHERE AND LIMIT 10"

        updated_uri, updated_operation = rest_api_adapter.RestAdapter.parse_operation_and_uri(uri, invalid_operation)

        assert uri == updated_uri
        assert invalid_operation == updated_operation

        path, query_params, headers_dict, fragment, body = rest_api_adapter.RestAdapter.parse_uri(updated_uri)
        assert path == "/api/gst-reports/reports/v3.0/ledgers/transaction"
        assert query_params == {}
        assert headers_dict == {}
        assert fragment == '$[*]'
        assert body == {}
