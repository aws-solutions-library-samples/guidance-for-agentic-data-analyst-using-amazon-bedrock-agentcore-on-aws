from agent import get_agent


def get_dataset_ids(tool_output):
    lines = []
    for line in tool_output.split('\n'):
        if line.startswith('[ID: '):
            lines.append(f" *  📊 {line}")
    return '\n'.join(lines)


class AgentClient:
    def __init__(self, keep_session_history):
        self.agent = get_agent()
        self.keep_session_history = keep_session_history
        self.session_history = []
        self.tool_uses = {}

    def add_session_history(self, message):
        if self.keep_session_history:
            self.session_history.append(message)

    def get_session_history(self):
        if not self.keep_session_history:
            return None
        return self.session_history

    def handle_message(self, user_message_text):
        user_message = {"msg_type": "user", "text": user_message_text}
        self.add_session_history({'role': 'user', 'content': [{'text': user_message_text}]})
        yield user_message
        
        for msg in self.agent.invoke(user_message_text, self.get_session_history()):
            if 'error' in msg:
                yield {"msg_type": "error", "text": msg['error']}
            
            elif msg['msg_type'] in {'datasets', 'result'}:
                yield msg
            
            elif msg['msg_type'] == 'message':
                message = msg['message']
                self.add_session_history(message)
                for content in message['content']:
                    if 'text' in content:
                        yield {"msg_type": "text", "text": content['text']}
                    
                    elif 'toolUse' in content:
                        toolUse = content['toolUse']
                        self.tool_uses[toolUse['toolUseId']] = toolUse['name']
                        toolUse["msg_type"] = "toolUse"
                        yield toolUse
                    
                    elif 'toolResult' in content:
                        toolResult = content['toolResult']
                        toolResult['name'] = self.tool_uses[toolResult['toolUseId']]
                        toolResult["msg_type"] = "toolResult"

                        if toolResult['name'] == 'search_datasets':
                            datasets = ["ONS Datasets relevant to the Agent query:"]
                            for result in toolResult['content']:
                                datasets.append(get_dataset_ids(result['text']))
                            toolResult['content'] = '\n'.join(datasets)
                        yield toolResult
