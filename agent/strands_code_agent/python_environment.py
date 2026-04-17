import warnings
from typing import Callable, Type, Union
from collections import defaultdict

from cryptography.utils import CryptographyDeprecationWarning
from smolagents.local_python_executor import LocalPythonExecutor
from strands import tool


STDOUT_LABEL = "STDOUT:\n"
STDERR_LABEL = "STDERR:"


def get_import_string(symbols: list[Union[Callable, Type]]) -> str:
    """Generate minimal import statements for a list of symbols."""
    by_module: dict[str, list[str]] = defaultdict(list)
    for sym in symbols:
        if sym.__module__ == "__main__":
            continue
        by_module[sym.__module__].append(sym.__qualname__.split(".")[0])

    lines = []
    for module, names in sorted(by_module.items()):
        lines.append(f"from {module} import {', '.join(sorted(set(names)))}")
    return "\n".join(lines)


class PythonInterpreter:
    def __init__(self, state_initialization=None, authorized_imports=None, additional_functions=None, stdout_label=STDOUT_LABEL, stderr_label=STDERR_LABEL):
        self.state_initialization = state_initialization
        self.stdout_label = stdout_label
        self.stderr_label = stderr_label
        self.authorized_imports = authorized_imports or []
        self.additional_functions = additional_functions or {}

        self._init_executor()

    def _init_executor(self):
        # smolagents' LocalPythonExecutor introspects all attributes of authorized modules
        # via getattr(), which triggers CryptographyDeprecationWarnings on deprecated
        # elliptic curves and cipher constants that emit warnings on access, not just use.
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
            self.executor = LocalPythonExecutor(
                additional_authorized_imports=self.authorized_imports,
                additional_functions=self.additional_functions,
            )
            self.executor.send_tools({})
            if self.state_initialization:
                self.executor(self.state_initialization)

    def clear_state(self):
        self._init_executor()

    def execute_code(self, code):
        stdout, stderr = "", ""
        try:
            result = self.executor(code)
            stdout = result.logs.strip() if result.logs else ""
        except Exception as e:
            # Salvage any print output captured before the error
            if hasattr(self.executor, "state") and "_print_outputs" in self.executor.state:
                stdout = str(self.executor.state["_print_outputs"]).strip()
            stderr = str(e)
        return stdout, stderr

    def get_tool(self):
        @tool
        def python_repl(code: str) -> str:
            """
            Executes Python code in a REPL environment with state persistence.

            Args:
                code: The Python code to execute
            """
            stdout_output, stderr_output = self.execute_code(code)

            observation = []
            if stdout_output:
                observation.append(f"{self.stdout_label}{stdout_output}")
            if stderr_output:
                observation.append(f"{self.stderr_label}{stderr_output}")
            if not observation:
                observation.append("Code executed successfully.")
            return '\n'.join(observation)

        return python_repl
    
    def __str__(self) -> str:
        buffer = []
        if self.authorized_imports:
            buffer.append(f"# Authorized Imports: {', '.join(self.authorized_imports)}")
        if self.additional_functions:
            buffer.append(f"# Additional Functions: {', '.join(self.additional_functions.keys())}")
        if self.state_initialization:
            buffer.append(f"""# Init Code:
```python
{self.state_initialization}
```
""")
        return '\n'.join(buffer) if buffer else "Standard PythonInterpreter"
