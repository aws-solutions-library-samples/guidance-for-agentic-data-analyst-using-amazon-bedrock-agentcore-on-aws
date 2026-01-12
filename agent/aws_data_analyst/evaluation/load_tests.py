from aws_data_analyst.evaluation import TESTS_PATH
from aws_data_analyst.python_environment import PythonInterpreter


def load_tests():
    interpreter = PythonInterpreter(stdout_label="")
    exec_tool = interpreter.get_tool()

    for dir_path in TESTS_PATH.iterdir():
        question_path = dir_path / 'question.txt'
        script_path = dir_path / 'script.py'
        if not question_path.exists() or not script_path.exists():
            continue
        
        question = question_path.read_text().strip()
        script = script_path.read_text().strip()

        interpreter.clear_state()

        answer = exec_tool(script)

        yield {
            'id': dir_path.name,
            'query': question,
            'answer': answer,
        }
