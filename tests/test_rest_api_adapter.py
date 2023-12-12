from rest_db_api import rest_api_adapter


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
