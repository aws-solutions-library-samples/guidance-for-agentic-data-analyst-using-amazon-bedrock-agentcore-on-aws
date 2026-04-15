"""
A toolkit provides a set of:
 - Libraries to be added to the list of authorized_imports of the LocalPythonExecutor
 - Snippet of code to initialize the python environment
 - Extra instructions to be included in the system prompt about how to use the provided libraries
 - Domain Specific Code to be fully imported and documented
"""


class Toolkit:
    def __init__(self,
                 libraries: list[str]|None=None,
                 initialization_code:str|None=None,
                 usage_instructions:str|None=None,
                 domain_specific_code:list|None=None):
        self.libraries = libraries
        self.initialization_code = initialization_code
        self.usage_instructions = usage_instructions
        self.domain_specific_code = domain_specific_code


VISUALIZATION_TOOLKIT = Toolkit(
    libraries = ['matplotlib', 'matplotlib.pyplot', 'seaborn'],
    initialization_code = """
# Visualization Libraries
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
""",
    usage_instructions="""
Do not try to show any matplotlib image: the python_repl tool executes the code in a sub-process without a GUI.
""")


NUMPY = Toolkit(
    libraries = ['numpy'],
    initialization_code = """
import numpy as np
""")


PANDAS = Toolkit(
    libraries = ['pandas'],
    initialization_code = """
import pandas as pd
""")


DATETIME = Toolkit(
    libraries = ['datetime'],
    initialization_code = """
from datetime import date, datetime, timedelta
""")
