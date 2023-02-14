from datetime import datetime
from babel.dates import format_date
from string import Template

import frappe
from frappe.translate import guess_language
from typing import List, Dict, Union, Optional, Set


def translate(source, lang):
    return _translate(source, lang)


def ftranslate(message: str, mapping: dict) -> str:
    """Translates a template string using frappe._"""
    return Template(frappe._(message)).substitute(mapping)


def _translate(source: any, language=None, sub_dict=None, exclusions: List[str] = None, inclusions: List[str] = None,
              debug=False) -> any:
    if exclusions is None and not inclusions:
        exclusions = ["id", "name"]

    if not language:
        language = get_request_language() if frappe.request else 'en'
    if language:
        if language == "en-US,en;q=0.5":
            language = "en"

    if isinstance(source, dict) or isinstance(source, list):
        keys = _extract_translation_keys(source, exclusions, inclusions, debug=debug)
        if not keys:
            if debug:
                print("No keys found")
            return source

        if debug:
            print(f'Keys: {keys}')
        translations = _translate_keys(keys, language)
        if debug:
            print(f'Translations: {translations}')
        visitor = _TranslationVisitor(translations, exclusions, inclusions)
        _visit(source, visitor)
        return source

    if isinstance(source, datetime):
        return format_date(source, format='full', locale=language)

    translation = _translate_single(source, language, sub_dict)

    return translation


def get_request_language():
    # TODO: This will method will need to be revamped. This fix decreased failed test cases to two.
    frappe.local.lang = frappe.request.headers.environ.get("HTTP_ACCEPT_LANGUAGE", "en")
    return guess_language(lang_list=["en", "ar"])


def _translate_single(source, lang, sub_dict=None):
    translation = None
    if isinstance(source, str):
        if not source.isdigit():
            translation = frappe.get_value("Translation",
                                           {"source_text": str(source).rstrip(), "language": lang},
                                           ["translated_text"])

    elif isinstance(source, int) or isinstance(source, float):
        return source

    res = translation if translation else source

    if sub_dict:
        res = Template(res).substitute(sub_dict)

    return res


def _extract_translation_keys(obj: Union[List[any], Dict[str, any]], exclusions=None, inclusions=None,
                              debug=False) -> List[str]:
    """Extracts a list of translation keys from a dictionary of string to objects

    :param obj - The dictionary
    :param exclusions - A list of keys to exclude from walking and extraction
    :param debug - A boolean indicating whether to print vebose debugging information
    """
    if exclusions is None:
        exclusions = []

    if inclusions is None:
        inclusions = []

    visitor = _KeyExtractionVisitor(exclusions, inclusions, debug=debug)
    _visit(obj, visitor)
    return list(visitor.keys)


def _translate_keys(keys: List[str], language: str) -> Dict[str, str]:
    """
    Takes a list of string keys and a language and returns a dictionary of translations (key -> translation).
    If a key has no translation it returns it as is.
    """
    query = """
    SELECT name, source_text, translated_text
    FROM tabTranslation
    WHERE source_text in %(keys)s AND language = %(language)s
    """
    rows: List[Dict[str, any]] = frappe.db.sql(query, {'keys': keys, 'language': language}, as_dict=True)
    translations = {row['source_text']: row['translated_text'] for row in rows}

    return translations


class _Visitor(object):
    """A visitor for an object hierarchy supporting dictionaries, lists, and string primitives"""

    def visit_dict(self, key: Optional[str], x: Dict[str, any]) -> bool:
        """ Visits a dictionary and returns a boolean indicating whether to visit the dictionary items

        :param key - The key of the dictionary in its parent if it has a dictionary parent
        :param x - The dictionary
        """
        return True

    def visit_list(self, key: Optional[str], x: List[any]) -> bool:
        """ Visits a list and returns a boolean indicating whether to visit the list items

        :param key - The key of the list in its parent if it has a dictionary parent
        :param x - The list
        """
        return True

    def visit_str(self, key: Optional[str], x: str) -> Optional[str]:
        """Visits a string and optionally returns a replacement value for the string

        :param key - The key of the string in its parent if it has a dictionary parent
        :param x - The string
        """
        pass


class _KeyExtractionVisitor(_Visitor):
    """A visitor that extracts translatable keys from an object"""

    def __init__(self, exclusions: List[str], inclusions: List[str] = None, debug=False):
        """Initializes a key extractor

        :param exclusions - A list of string keys to exclude from walking and key extraction
        :param inclusions - A list of string keys to only include. Mutually exclusive with and overrides exclusions
        :param debug - A boolean indicating whether to print verbose debugging information
        """
        self.keys: Set[str] = set()
        self.exclusions = set(exclusions)
        self.inclusions = set(inclusions) if inclusions else set()
        self.debug = debug
        if self.inclusions:
            self._should_exclude = lambda key: not self.inclusions.__contains__(key)
        else:
            self._should_exclude = lambda key: self.exclusions.__contains__(key)

    def visit_dict(self, key: Optional[str], x: Dict[str, any]) -> bool:
        excluded = key and self._should_exclude(key)
        if self.debug:
            print(f'Exclusion: {key} -> {excluded}')
        return not excluded

    def visit_list(self, key: Optional[str], x: List[any]) -> bool:
        excluded = key and self._should_exclude(key)
        if self.debug:
            print(f'Exclusion: {key} -> {excluded}')
        return not excluded

    def visit_str(self, key: Optional[str], x: str) -> Optional[str]:
        if key and self._should_exclude(key):
            if self.debug:
                print(f'Ignoring key: {key}')
        elif x.isdigit():
            if self.debug:
                print(f'Ignoring numerical value: {x}')
        else:
            self.keys.add(x)

        return None


class _TranslationVisitor(_Visitor):
    """A visitor that translates an object hierarchy"""

    def __init__(self, translations: Dict[str, str], exclusions: List[str] = None, inclusions: List[str] = None):
        """Initializes an instance of the translation visitor

        :param translations - A dictionary of string to string translations
        :param exclusions - A list of keys to ignore when walking and translating the object hierarchy
        :param inclusions - A list of keys to translate. Mutually exclusive with exclusions and takes precedence over it
        """
        self.translations = translations
        self.exclusions = set(exclusions) if exclusions else set()
        self.inclusions = set(inclusions) if inclusions else set()
        if self.inclusions:
            self._should_exclude = lambda key: not self.inclusions.__contains__(key)
        else:
            self._should_exclude = lambda key: self.exclusions.__contains__(key)

    def visit_dict(self, key: Optional[str], x: Dict[str, any]) -> bool:
        excluded = key and self._should_exclude(key)
        return not excluded

    def visit_list(self, key: Optional[str], x: List[any]) -> bool:
        excluded = key and self._should_exclude(key)
        return not excluded

    def visit_str(self, key: Optional[str], x: str) -> Optional[str]:
        if key and self._should_exclude(key):
            return None

        return self.translations.get(x) or x


def _visit(obj: any, visitor: _Visitor) -> None:
    """Visits an object hierarchy and updates it based on the given visitor.

    Supports dictionaries, lists, and string primitives
    """

    def dispatch(k: Union[int, str, None], v: any, parent: Union[list, dict, None]):
        # k is either a dictionary key (which can be anything) or a list index. Visitors only accept and filter on
        # string keys. We pass a key of None for everything else
        filter_key = k if isinstance(k, str) else None

        if isinstance(v, dict):
            if visitor.visit_dict(filter_key, v):
                walk_dict(v)
        elif isinstance(v, list):
            if visitor.visit_list(filter_key, v):
                walk_list(v)
        elif isinstance(v, str):
            new_v = visitor.visit_str(filter_key, v.rstrip())
            if new_v and new_v != v:
                parent[k] = new_v

    def walk_dict(d: Dict[str, any]):
        for k, v in d.items():
            dispatch(k, v, d)

    def walk_list(ls: List[any]):
        for i, v in enumerate(ls):
            dispatch(i, v, ls)

    dispatch(None, obj, None)
