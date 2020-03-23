import inspect
import logging
import pkgutil
import types
from importlib import import_module

from airflow import operators
from airflow.contrib import operators as contrib_operators

from .operator_handler import OperatorHandler
from ...exceptions import OperatorMarshallError


class OperatorIndex:
    def __init__(self, custom_operators=""):
        """Stateful object to index built-in and custom airflow
        operators

        Args:
            custom_operators (str, optional): Path to directory containing 
                custom operators. Defaults to "".
        """

        self.custom_operators = custom_operators
        self.operator_list = self.get_operators()

    def marshall_operator_list(self):
        """Return a JSON marshalled list of Operators as per OperatorHandler schema
        
        Returns:
            List[Dict]: List of OperatorHandler Dict - see `schemas.app_schemas.OperatorSchema`
        """
        handlers = []
        for operator in self.operator_list:
            try:
                handlers.append(OperatorHandler.from_operator(operator))
            except OperatorMarshallError as e:
                logging.exception(f"Unable to parse operator {operator.__name__}: {e}")

        return [h.dump() for h in handlers]

    def get_operators(self):
        """Get all default and custom operators
        
        Returns:
            List[Operator]: List of classes that inherit from BaseOperator
        """
        return self.get_default_operators()

    @staticmethod
    def get_modules(path, route):
        modules = []
        for _, modname, _ in pkgutil.iter_modules(path):
            try:
                base_mod = import_module(f"{route}.{modname}")
                modules.extend(list(base_mod.__dict__.values())) 
            except (ModuleNotFoundError, SyntaxError) as e:
                # NOTE Some of the HDFS libraries in Airflow don't support Python 3
                logging.info(f"Unable to import operator from {modname}: {e}")
        return modules


    @staticmethod
    def get_default_operators():
        """Scrapes operators module for all classes that inherit from the BaseOperator
        class
        
        Returns:
            List[Operator]: List of Operator classes
        """
        ops = set()
        modules = OperatorIndex.get_modules(operators.__path__, "airflow.operators") + \
                  OperatorIndex.get_modules(contrib_operators.__path__, "airflow.contrib.operators")
        ops = ops.union(
            {
                v
                for v in modules
                if inspect.isclass(v) and issubclass(v, operators.BaseOperator)
            }
        )

        return list(ops)
