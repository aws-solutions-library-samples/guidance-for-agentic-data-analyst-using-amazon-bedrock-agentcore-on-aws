import tempfile

from strands import Agent
from jinja2 import Template

from strands_code_agent.document_code import get_documentation
from strands_code_agent.python_environment import get_import_string, PythonInterpreter


CODE_AGENT_INSTRUCTIONS = """
You are a code agent. You solve tasks by writing and executing Python code using the python_repl tool.

The Python interpreter state resets completely with each new user message, but it persists across multiple tool invocations within a single response.
You can perform multi-step computation within a single turn, but do not assume that results from a previous turn are still in memory.
"""

CODE_PREAMBLE_TEMPLATE = Template("""
The python environment of the python_repl tool is initialised with the following code (you do not need to rewrite this code):
```python
{{CODE_PREAMBLE}}
```
""")

TEMP_DIR_TEMPLATE = Template("""
If you need to generate a file use this temporary directory: {{TEMP_DIR}}
The user has no access to this temporary directory.
""")

DOMAIN_SPECIFIC_DOC_TEMPLATE = Template("""
You can use the following Domain Specific Code:
{{SYMBOLS_DOCUMENTATION}}
""")


class CodeAgent(Agent):
    """A coding agent that extends Strands Agent with a Python REPL and domain-specific symbol documentation.

    Args:
        system_prompt: the system prompt to be expanded with the coding instructions.

        tools: additional tools to include alongside the built-in Python REPL.

        toolkits: additional libraries, initialization code, usage instructions and Domain Specific Code.
                
        tmp_dir: If True, creates a temporary directory and documents its path in the system prompt.

        **kwargs: Additional arguments forwarded to the Strands ``Agent`` base class.
    """

    def __init__(self,
                 system_prompt:str|None,
                 tools:list|None,
                 toolkits:list|None,
                 tmp_dir=True,
                 **kwargs):
        authorized_imports = set()
        initialization_code = []
        usage_instructions = []
        domain_specific_code = []
        if toolkits is not None:
            for toolkit in toolkits:
                if toolkit.libraries is not None:
                    authorized_imports.update(toolkit.libraries)
                if toolkit.initialization_code is not None:
                    initialization_code.append(toolkit.initialization_code.strip())
                if toolkit.usage_instructions is not None:
                    usage_instructions.append(toolkit.usage_instructions.strip())
                if toolkit.domain_specific_code is not None:
                    domain_specific_code.extend(toolkit.domain_specific_code)

        additional_functions = {}
        domain_specific_doc = ""
        if domain_specific_code:
            authorized_imports.update(sym.__module__ for sym in domain_specific_code)
            initialization_code.append(get_import_string(domain_specific_code))
            sym_doc =  "\n".join([get_documentation(sym) for sym in domain_specific_code])
            domain_specific_doc = DOMAIN_SPECIFIC_DOC_TEMPLATE.render(SYMBOLS_DOCUMENTATION=sym_doc)
            additional_functions = {sym.__qualname__.split(".")[0]: sym for sym in domain_specific_code}
        
        code_preamble = "\n".join(initialization_code)
        if code_preamble:
            code_preamble_doc = CODE_PREAMBLE_TEMPLATE.render(CODE_PREAMBLE=code_preamble)

        tmp_dir_doc = ""
        if tmp_dir:
            self.tmp_dir = tempfile.mkdtemp(dir='/tmp')
            tmp_dir_doc = TEMP_DIR_TEMPLATE.render(TEMP_DIR=self.tmp_dir)

        system_prompt = '\n'.join([
            system_prompt if system_prompt is not None else "",
            CODE_AGENT_INSTRUCTIONS,
            code_preamble_doc,
            "\n".join(usage_instructions),
            tmp_dir_doc,
            domain_specific_doc
        ])

        self.python_repl = PythonInterpreter(
            code_preamble,
            authorized_imports=authorized_imports,
            additional_functions=additional_functions,
        )
        python_repl_tool = self.python_repl.get_tool()
        if tools is not None:
            tools.append(python_repl_tool)
        else:
            tools = [python_repl_tool]
        
        kwargs.update({
            "system_prompt": system_prompt,
            "tools": tools
        })
        super().__init__(**kwargs)