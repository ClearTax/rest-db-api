import json
import logging
import re
import urllib
from typing import Dict, Any, Tuple, List, Union
from urllib.parse import urlencode

import sqlglot

_logger = logging.getLogger(__name__)

white_listed_header_params: dict[str, str] = {
    "org_id": "x-cleartax-orgunit",
    "node_ids": "x-clear-node-id",
    "cookie": "cookie",
    "node_type": "x-clear-node-type"
}

white_listed_query_params: dict[str, str] = {

}

invalid_sql_clause_post_parsing: list[str, str] = [
    r'\bAND\s*\(\s*\)',
    r'\bWHERE\s*\(\s*\)',
]


def get_virtual_table(endpoint: str,
                      params: Dict[str, Any] = None,
                      headers: Dict[str, Any] = None,
                      body: Dict[str, Any] = None,
                      jsonpath: str = "$[*]") -> str:
    params_str = get_params_str(params=params)

    headers_custom_param = get_custom_header_params(is_param_added=True if params else False,
                                                    headers=headers)

    is_param_added = False
    if params or headers:
        is_param_added = True
    custom_body_param = get_custom_body_param(is_param_added=is_param_added,
                                              body=body)

    virtual_table = endpoint + params_str + headers_custom_param + custom_body_param + "#" + jsonpath
    return virtual_table


def get_custom_header_params(is_param_added: bool = False, headers: Dict[str, Any] = None) -> str:
    if not headers:
        return ''

    if is_param_added:
        headers_custom_param = '&'
    else:
        headers_custom_param = '?'

    header_number = 1
    loop = len(headers)
    for key, value in headers.items():
        headers_custom_param += 'header' + str(header_number) + "=" + key + ":" + value
        if header_number < loop:
            headers_custom_param += "&"
        header_number += 1

    return headers_custom_param


def get_params_str(params: Dict[str, Any] = None) -> str:
    if not params:
        return ''
    params_str = "?" + urlencode(params)
    return params_str


def get_custom_body_param(is_param_added: bool = False, body: Dict[Any, Any] = None) -> str:
    if not body:
        return ''

    if is_param_added:
        custom_body_param = '&body='
    else:
        custom_body_param = '?body='
    json_parsed_body = json.dumps(body)
    custom_body_param += urllib.parse.quote(json_parsed_body, 'utf8')
    return custom_body_param


def remove_invalid_clause(operation: str) -> str:
    for invalid_clause in invalid_sql_clause_post_parsing:
        operation = re.sub(invalid_clause, '', operation)

    return operation


def sanitize_string(string: str) -> str:
    if string.startswith("'") or string.endswith("'"):
        return string.strip("'")
    elif string.startswith('"') or string.endswith('"'):
        return string.strip('"')

    return string


def append_to_uri(uri: str, name: str, value: str) -> str:
    if "?" not in uri:
        uri += "?"
    elif not uri.endswith("&"):
        uri += "&"

    if name in white_listed_header_params:
        name = "HEADER" + white_listed_header_params[name]
    elif name in white_listed_query_params:
        name = "QUERY_PARAM" + white_listed_query_params[name]

    if name.startswith("QUERY_PARAM"):
        name = name.removeprefix("QUERY_PARAM") + "="
    elif name.startswith("HEADER"):
        name = "header=" + name.removeprefix("HEADER") + ":"

    return uri + name + value


def append_headers_and_query_params_to_uri(expression: Union[sqlglot.expressions.Expression, List], is_where_clause,
                                           uri) -> str:
    if expression:
        if isinstance(expression, list):
            for i in expression:
                uri = append_headers_and_query_params_to_uri(i, is_where_clause, uri)
        elif isinstance(expression, sqlglot.expressions.And):
            for key, value in list(expression.args.items()):
                uri = append_headers_and_query_params_to_uri(value, is_where_clause, uri)

            if not len(expression.args.items()):
                expression.pop()
        elif isinstance(expression, sqlglot.expressions.EQ):
            eq_expression: sqlglot.expressions.EQ = expression
            if is_where_clause:
                param_name = sanitize_string(eq_expression.left.sql())
                if (param_name.startswith("QUERY_PARAM") or param_name.startswith("HEADER")
                        or param_name in white_listed_header_params or param_name in white_listed_query_params):
                    param_value = sanitize_string(eq_expression.right.sql())
                    uri = append_to_uri(uri, param_name, param_value)
                    eq_expression.pop()
        elif isinstance(expression, sqlglot.expressions.In):
            in_expression: sqlglot.expressions.In = expression
            if is_where_clause:
                param_name = sanitize_string(in_expression.this.sql())
                if (param_name.startswith("QUERY_PARAM") or param_name.startswith("HEADER")
                        or param_name in white_listed_header_params or param_name in white_listed_query_params):
                    query_param_values = [sanitize_string(value.sql()) for value in in_expression.expressions]
                    for param_value in query_param_values:
                        uri = append_to_uri(uri, param_name, param_value)
                    expression.pop()
        elif isinstance(expression, sqlglot.expressions.Expression):
            for key, value in list(expression.args.items()):
                uri = append_headers_and_query_params_to_uri(value, is_where_clause or isinstance(expression,
                                                                                                  sqlglot.expressions.Where),
                                                             uri)

    return uri.strip()


def parse_operation_and_uri(uri: str, operation: str) -> Tuple[str, str]:
    parsed_operation = sqlglot.parse_one(sqlglot.parse_one(operation).sql())
    updated_uri = append_headers_and_query_params_to_uri(parsed_operation, False, uri)

    operation_elems = []
    for index, value in enumerate(parsed_operation.sql().split()):
        if value == 'AND':
            while len(operation_elems) > 0 and operation_elems[-1] == 'AND':
                operation_elems.pop()
            if len(operation_elems) > 0 and operation_elems[-1] != 'WHERE':
                operation_elems.append(value)
        elif value == 'ORDER' or value == 'LIMIT' or value == ")":
            while len(operation_elems) > 0 and (operation_elems[-1] == 'WHERE' or operation_elems[-1] == 'AND'):
                operation_elems.pop()
            operation_elems.append(value)
        else:
            operation_elems.append(value)

    while len(operation_elems) > 0 and (operation_elems[-1] == "WHERE" or operation_elems[-1] == 'AND'):
        operation_elems.pop()

    updated_operation = remove_invalid_clause(" ".join(operation_elems))

    updated_operation = updated_operation.replace(uri, updated_uri)

    return updated_uri.strip(), sqlglot.parse_one(updated_operation.strip()).sql()
