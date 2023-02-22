from __future__ import with_statement

import datetime
import inspect
import json
import traceback
from io import SEEK_END, SEEK_SET
from string import Template
from typing import Dict, Optional, Tuple, List, Callable

import frappe
from frappe.utils.data import get_datetime
from werkzeug.exceptions import BadRequest, NotFound

from . import insights
from .utils.input_model_base import POSException
from .utils.translation import get_request_language


def api_version(version: int) -> Callable:
    """API version decorator. Used to indicate a given ApiEndpoint method is a specific API
    version. """

    def inner_version(fn):
        fn.__api_version = version
        return fn

    return inner_version


def check_parameters(params: list):
    incorrect_data = []
    for param in params:
        values = param["values"]
        if values:
            if isinstance(values, str):
                values = json.loads(values)
            wrong_values = []
            for v in values:
                d = frappe.get_value(param["doctype"], {"name": v}, ["name"])
                if not d:
                    wrong_values.append(v)
            if wrong_values:
                doctype = param["doctype"]
                incorrect_data.append({f"Wrong {doctype}": wrong_values})
    return incorrect_data


def validate_select_fields_value(field_name, doctype, value):
    options = frappe.get_value("DocField", {"fieldname": field_name, "parent": doctype, "fieldtype": "Select"},
                               ["options"])
    options = [d for d in options.split('\n') if d]
    if value not in options:
        return False, options
    else:
        return True, []


class AsAdmin:
    def __init__(self):
        self.user = frappe.session.user

    def __enter__(self):
        frappe.session.user = "administrator"

    def __exit__(self, exc_type, exc_val, exc_tb):
        frappe.session.user = self.user


class ApiEndpoint(object):
    """Base class for all API endpoints."""

    def __init__(self, name, sensitive_keys=None, impersonate_user=False):
        """Initializes the API endpoint with the given name. The name will be used for analytics."""
        self.name = name
        self.logger = insights.get_logger(name)
        self.sensitive_keys = sensitive_keys if sensitive_keys else ['password']
        self.impersonate_user = impersonate_user
        self.logged_parameter_sources = {}

    def impersonate(self) -> bool:
        user_id = self.get_impersonated_user_id()
        if not user_id or not frappe.db.exists('User', user_id):
            self.logger.error("Can't impersonate non-existing user")
            return False

        # Users are allowed to impersonate themselves. This happens when a user calls an impersonating API from his
        # own session (e.g. trigger a whitelisted impersonating function from Desk)
        if frappe.session.user == user_id:
            self.logger.debug(f'User {frappe.session.user} impersonating himself')
            return True

        # Only the system manager can impersonate other users (e.g. LavaHub)
        if 'System Manager' not in frappe.get_roles():
            self.logger.error(f'Attempt to impersonate by non-system-manager user: {frappe.session.user}')
            return False

        self.logger.info(f'Impersonating {user_id}')
        # Setting the session user is not enough, because permissions could have been cached. We use set_user for that,
        # but we should keep an eye out if that's not enough either
        frappe.session.user = user_id

        # TODO: Find a better solution for this hot fix.
        # frappe.set_user(user_id) is essential to clear any permissions related to the admin/hub user, but at the same
        # time it caused a problem, where for every api call it is needed to call the login api first or it will raise
        # an error "User None not found, The resource you are looking for is not available."
        #
        # A proper fix would recreate all data used to create the user session as seen in Session.start or
        # Session.resume instead of this hack which muddies session data
        _data = frappe.session.data
        _data.user = user_id

        frappe.set_user(user_id)
        frappe.session.data = _data
        return True

    def get_impersonated_user_id(self):
        """
            Searches for user_id in the request then removes it from the request data
        """
        query_string_params = self.query_string()
        user_id = query_string_params.get("user_id", None)

        if not user_id:
            json_params = self.json_body()
            user_id = json_params.get("user_id", None)

        return user_id

    def log_parameters(self, source: str, params: dict):
        # Avoid logging the same source multiple times until we refactor parameter logging
        # TODO: Fix parameter logging to avoid calling it multiple times
        if source in self.logged_parameter_sources:
            return
        self.logged_parameter_sources[source] = True

        logged_params = {k: '****' if k in self.sensitive_keys else v for k, v in params.items()}
        if logged_params:
            self.logger.info(f"parameters ({source}): {logged_params}")

    def run(self, *args, **kwargs):
        """Entry-point for this endpoint. Parses API version from query string or body and calls
        the appropriate implementation, or the default implementation."""

        with insights.tracer.span(self.name):
            self.logger.info('START {}'.format(self.name))
            if self.impersonate_user:
                if not self.impersonate():
                    return self.respond_with_code(code=403, error_code="Forbidden")

            requested_api_version = None
            self.log_parameters('kwargs', kwargs)

            if 'api_version' in kwargs:
                requested_api_version = kwargs['api_version']
                self.logger.info(
                    'API version ''{}'' from query string'.format(requested_api_version))

            if requested_api_version is None:
                requested_api_version = self.try_get_api_version_from_query_and_body()
                if requested_api_version:
                    self.logger.info('API version ''{}'' from body'.format(requested_api_version))

            self.api_version = requested_api_version
            if requested_api_version is not None:
                method = self.get_method_by_api_version(requested_api_version)
                if not method:
                    self.logger.info('Could not find implementation for version ''{}'''.format(
                        requested_api_version))
                    return self.respond_with_code(code=400, error_code='InvalidVersion',
                                                  message="Invalid API Version '{}'".format(
                                                      requested_api_version))

                result = self.execute(lambda: method(*args, **kwargs))
            else:
                self.logger.info('No API version requested. Executing default version.')
                result = self.execute(lambda: self.default(*args, **kwargs))

            self.logger.info('END {}'.format(self.name))
            return result

    def default(self, *args, **kwargs):
        """Default endpoint implementation if no API version is specified. Override this in derived
        classes."""
        return self.respond_with_code(code=200, data={})

    def query_string(self):
        """Return a dictionary from the Query string."""
        self.log_parameters('query', frappe.request.args)
        return frappe.request.args

    def json_body(self):
        """Returns a dictionary parsed from the body as JSON. If the body is not json, returns an
        empty dictionary."""
        body = frappe.request.data or '{}'
        try:
            result = json.loads(body)
        except ValueError:
            result = dict()

        self.log_parameters('json', result)
        return result

    def form_body(self):
        """Returns a dictionary representing the form body parameters including files."""
        body = frappe.request.form.copy()

        # Add files directly as well. Iterating yields the file parameter names. Indexing with the
        # name yields a werkzeug.datastructures.FileStorage which we use as the value
        for f in frappe.request.files:
            body[f] = frappe.request.files[f]

        self.log_parameters('form', body)
        return body

    def file_size_in_bytes(self, f):
        f.seek(0, SEEK_END)
        size = f.tell()
        f.seek(0, SEEK_SET)
        return size

    def try_get_api_version_from_query_and_body(self):
        """Attempts to retrieve the API version from the query string, then request body if any. Treats the body as
        JSON first, then form-data if JSON is empty."""
        params = self.query_string()
        if 'api_version' in params:
            return params['api_version']

        params = self.json_body()
        if not params:
            params = self.form_body()

        if 'api_version' in params:
            return params['api_version']

        return None

    def get_method_by_api_version(self, version):
        """Inspects the current object for methods decorated with @api_version and whose declared
        version matches the requested version."""
        methods = [f[1] for f in inspect.getmembers(self, inspect.ismethod) if
                   hasattr(f[1], '__api_version') and str(
                       getattr(f[1], '__api_version')) == str(version)]
        if not methods:
            return None

        if len(methods) > 1:
            insights.get_logger(__file__).error(
                "Multiple methods found for API version '{}'. Will use the first one.".format(
                    version))

        return methods[0]

    def validate_date_format(self, dates, date_format="%Y-%m-%d"):
        try:
            for date in dates:
                datetime.datetime.strptime(date, date_format)
            return True
        except ValueError:
            return False

    def validate_required_parameters(self, params: Dict[str, any], required_parameter_names: List[str] = None,
                                     alternative_parameters: Optional[List[Tuple]] = None) -> \
            Tuple[bool, Optional[Dict[str, any]]]:
        """Checks that all keys defined in 'required_parameter_names' exist in 'params', and that
        one of the parameter names in each tuple in 'alternative_parameters' exists in 'params'.
        * params: The incoming parameters from body or query string as a dictionary
        * required_parameter_names: A list of required parameter names
        * alternative_parameters: A list of tuples, each defining the names of alternative parameter
        names (that is, if one of the names in the tuple is defined, the requirement is met)"""

        def check_tuple_parameters(param_tuple):
            for p in param_tuple:
                if p in params and params[p] is not None:
                    return True
            return False

        alternative_parameters = alternative_parameters if alternative_parameters else []
        required_parameter_names = required_parameter_names if required_parameter_names else []

        if not alternative_parameters and not required_parameter_names:
            raise Exception("Must send one of theses lists alternative_parameters or required_parameters_name "
                            "to validate_required_parameters.")

        missing_parameters = [p for p in required_parameter_names if
                              p not in params or params[p] is None]

        missing_alt_parameters = [p for p in alternative_parameters if
                                  not check_tuple_parameters(p)]

        if missing_alt_parameters or missing_parameters:
            message = ''
            if missing_parameters:
                message += 'Required parameters are missing: {}. '.format(
                    ', '.join(missing_parameters))

            if missing_alt_parameters:
                tuples = [' | '.join(x) for x in [p for p in missing_alt_parameters]]
                message += 'Specify at least one of the following: {}'.format(', '.join(tuples))

            return False, self.respond_with_code(code=400, error_code='ArgumentNotFound',
                                                 message=message)

        return True, None

    def validate_required_parameters_has_vales(self, params, required_parameters):
        missing_values = []
        for p in required_parameters:
            v = params.get(p)
            if v in ['0', '0.0', 0, 0.0]:
                return True, None

            if not v:
                missing_values.append(p)
        if missing_values:
            return False, self.respond_with_code(code=400, error_code='ValuesNotFound',
                                                 message=f"These arguments {missing_values} values can't be empty")
        return True, None

    def validate_input_type(self, input_params: dict, input_type: tuple, check_digit=False):
        wrong_type_values = []
        for k, v in input_params.items():
            if v:
                if not isinstance(v, input_type):
                    wrong_type_values.append(k)
                if str in input_type and check_digit and isinstance(v, str):
                    try:
                        float(v)
                    except ValueError:
                        wrong_type_values.append(k)
        if wrong_type_values:
            return False, self.respond_with_code(error_code="BadRequest",
                                                 developer_message=f"{wrong_type_values} have wrong input type",
                                                 code=400)
        return True, None

    def validate_positive_value(self, input_params: dict):
        negative_value = []
        for k, v in input_params.items():
            if v:
                if float(v) < 0:
                    negative_value.append(k)
        if negative_value:
            return False, self.respond_with_code(error_code="BadRequest",
                                                 developer_message=f"{negative_value} value shouldn't be negative",
                                                 code=400)
        return True, None

    def respond_with_code(
            self,
            message='',
            data=None,
            code=200,
            error_code='',
            exception=None,
            related_doctypes=None,
            developer_message='', sub_dict=None
    ) -> Dict[str, any]:
        """Responds with the standard envelope and the given status code, data, error code,
        and message.
       """
        if sub_dict is None:
            sub_dict = {}
        has_request = frappe.request
        if has_request:
            frappe.local.response.http_status_code = code
        developer_message = developer_message or message

        if exception and not error_code:
            error_code = exception.__class__.__name__

        if error_code and not message:
            error = frappe.get_value("Error Code", {'name': error_code}, ['message', 'http_code'],
                                     as_dict=True)

            if not error:
                message = f"Server Error ({error_code})"
            else:
                message = error.message

                if not code:
                    code = error.http_code

        if has_request:
            language = get_request_language() or 'en'
        else:
            language = 'en'

        if language:
            if error_code:
                org_message = message
                message = Template(translate.translate(error_code, language)).substitute(sub_dict)
                if message == error_code:
                    message = org_message
            else:
                message = Template(translate.translate(message, language)).substitute(sub_dict)
        self.logger.info(
            f"Respond with: Code = {code}, ErrorCode = {error_code}, Message = {message}, Developer Message = {developer_message}")

        return {
            "message": message,
            "data": data,
            "errorCode": error_code,
            "code": code,
            "developer_message": developer_message
        }

    def execute(self, action):
        """Executes an action and serializes frappe/web exceptions using the standard envelope."""
        try:
            return action()
        except frappe.ValidationError as e:
            traceback.print_exc()
            self.logger.exception('Validation error exception')
            frappe.db.rollback()
            return self.respond_with_code(exception=e, code=417)
        except NotFound as e:
            traceback.print_exc()
            self.logger.exception('Not found exception')
            frappe.db.rollback()
            return self.respond_with_code(exception=e, code=404)
        except BadRequest as e:
            self.logger.exception('Bad request exception')
            traceback.print_exc()
            frappe.db.rollback()
            return self.respond_with_code(exception=e, code=400)
        except (frappe.PermissionError, PermissionError) as e:
            self.logger.exception('Permission exception')
            traceback.print_exc()
            frappe.db.rollback()
            return self.respond_with_code(exception=e, code=403, error_code="Forbidden")
        except POSException as e:
            self.logger.exception("POS Exception")
            traceback.print_exc()
            frappe.db.rollback()
            return self.respond_with_code(exception=e, developer_message=str(e), code=417)
        except Exception as e:
            self.logger.exception('Generic exception')
            traceback.print_exc()
            frappe.db.rollback()
            return self.respond_with_code(exception=e, code=500)

    def get_paging_offset(self, args: dict) -> int:
        return self.get_int_value(args, 'offset', 0)

    def get_paging_count(self, args: dict) -> int:
        return self.get_int_value(args, 'count', 20)

    def get_int_value(self, args: dict, key: str, default: int) -> int:
        val = args.get(key, default)
        if isinstance(val, int):
            return val

        try:
            return int(val)
        except ValueError:
            return default

    def parse_from_string(self, param: any, data_type: type):
        if data_type in [int, float, str]:
            return data_type(param)
        elif data_type == datetime.datetime or data_type == datetime.date:
            return get_datetime(param)
        elif data_type == bool:
            return self._str_bool(param)
        elif data_type == list or data_type == dict:
            return json.loads(param)
        else:
            raise Exception('Invalid data type')

    def _str_bool(self, var):
        if isinstance(var, bool) or isinstance(var, int):
            return var
        if var.lower() == "true":
            return True
        return False

    def _translated_object(self, doctype=None, id=None, field=None):
        from lava_custom.utils.translation import translate as trans
        if doctype and id and field:
            res = {
                "id": id,
                "title": id
            }
            try:
                doc = frappe.get_doc(doctype, id)
                res.update({
                    "title": trans(doc.get(field))
                })
            finally:
                return res

        elif field and not doctype and not id:
            return {
                "id": field,
                "title": trans(field)
            }
        else:
            return {
                "id": id,
                "title": id
            }
