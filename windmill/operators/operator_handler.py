import logging
import re

from airflow.operators import BaseOperator
from docstring_parser import parse
from marshmallow import MarshalResult, UnmarshalResult

from ..schemas.app_schemas import (
    OperatorSchema,
    validate_parameter_type,
    VALID_PARAMETER_TYPES,
)
from ..utils.exceptions import OperatorMarshallError


def fix_types(typ: str):
    pass


def fix_docstring(docs: str):
    """Helper to convert Airflow Operator.__doc__ into valid docstrings so 
    docstring_parser can pick up types.

    Args:
        docs (str): Valid Airflow docstring, eg
            Some description...

            :param param1: some param
            :type param1: e.g. str
            ...

    return str: Reformatted str eg
            Some description...

            :param str param1: some param
            :type param1: e.g. str
            ...
    """
    type_re = r":type (.*): (.*)"

    for param, typ in re.findall(type_re, docs):
        if typ.lower().startswith("a "):
            typ = typ[2:]
        for root in VALID_PARAMETER_TYPES:
            if typ.startswith(root):
                typ = root
        if not validate_parameter_type(typ):
            logging.warning(f"Unable to parse field {typ}")
            typ = "str"
        docs = docs.replace(f":param {param}:", f":param {typ} {param}:")
    return docs


class OperatorHandler:
    """Handler to translate between Mashall Schemas, Operator Objects and
    dictionaries
    """

    schema = OperatorSchema()

    def __init__(self, type: str, properties: dict):
        """Should initialise using the from_marsh or from_operator method

        Args:
            type (str):  Operator Name
            properties (dict): Operator Properties
        """
        self.type = type
        self.properties = properties

    def dump(self):
        """Serialise to Marshalled Object

        Returns:
            MarshalResult: Named tuple with fields 'data' and 'errors'
        """
        res = self.schema.dump(self)
        if res.errors:
            raise OperatorMarshallError(f"Error marshalling {self.type}: {res.errors}")
        return res.data

    @staticmethod
    def from_operator(operator: BaseOperator.__class__):
        """Instantiate a handler from an Airflow Operator

        Arguments:
            operator {BaseOperator} -- Airflow Operator Class
        """
        fixed_docs = fix_docstring(operator.__doc__)
        docs = parse(fixed_docs)
        return OperatorHandler(
            operator.__name__,
            {
                "parameters": [
                    {
                        "id": p.arg_name,
                        "type": p.type_name,
                        "description": p.description,
                    }
                    for p in docs.params
                ]
            },
        )

    @staticmethod
    def from_dict(d: dict):
        """[summary]
        
        Args:
            d (dict): Unparsed dict
        
        Returns:
            OperatorHandler: Parsed operator handler
        """
        return OperatorHandler.from_marsh(OperatorHandler.schema.load(d))

    @staticmethod
    def from_marsh(res: UnmarshalResult):
        """Instantiate a handler from an Unmarshal Result

        Args:
            data (UnmarshalResult): Result from Marshmallow.load
        
        Returns:
            OperatorHandler: Parsed operator handler
        """
        return OperatorHandler(res.data["type"], res.data["properties"])
