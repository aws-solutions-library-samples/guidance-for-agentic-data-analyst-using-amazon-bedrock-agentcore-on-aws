import io
from contextlib import redirect_stdout, redirect_stderr

from strands import tool


class PythonInterpreter:
    def __init__(self, state_initialization=None):
        self.state_initialization = state_initialization
        self.state = {}
        if self.state_initialization:
            self.execute_code(self.state_initialization)
    
    def clear_state(self):
        self.state.clear()
        if self.state_initialization:
            self.execute_code(self.state_initialization)
    
    def execute_code(self, code):
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exec(code, self.state)
        return stdout_buffer.getvalue().strip(), stderr_buffer.getvalue().strip()

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
                observation.append(f"STDOUT:\n{stdout_output}")
            if stderr_output:
                observation.append(f"STDERR: {stderr_output}")
            if not observation:
                observation.append("Code executed successfully.")
            return '\n'.join(observation)

        return python_repl