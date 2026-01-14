import streamlit as st

from auth import Auth
from agent_client import AgentClient


# User Authentication
if "auth" not in st.session_state:
    st.session_state.auth = Auth()
authenticator = st.session_state.auth.authenticator
if not authenticator.login():
    st.stop()


# Agent
if "agent" not in st.session_state:
    st.session_state.agent = AgentClient(keep_session_history=False)
agent = st.session_state.agent


def format_dataset_item(dataset):
    return f" * 📊 [{dataset['key']}](https://www.ons.gov.uk/datasets/{dataset['key']}): {dataset['title']}"


# Chat
chat_container = st.container()


def agent_chat(user_prompt):
    chat_container.empty()
    for msg in agent.handle_message(user_prompt):
        display_message(msg)


def display_message(msg):
    if 'msg_type' not in msg:
        st.error(f"Unknown Message: {msg}")
        return
    
    with chat_container:
        if msg['msg_type'] == 'user':
            with st.chat_message('user'):
                st.markdown(msg['text'])
        elif msg['msg_type'] == 'error':
            st.error(msg['text'])
        else:
            with st.chat_message('assistant'):
                match msg['msg_type']:
                    case 'datasets':
                        datasets = msg['datasets']
                        buffer = ["Relevant ONS Datasets:"]
                        for dataset in datasets['entries']:
                            buffer.append(format_dataset_item(dataset))
                        st.markdown('\n'.join(buffer))
                    case 'text':
                        st.markdown(msg['text'])
                    case 'toolUse':
                        if msg['name'] == 'python_repl':
                            st.code(msg['input']['code'], language="python")
                        elif msg['name'] == 'search_datasets':
                            query = msg['input']['query']
                            st.markdown(f'🛠️ Search extra datasets for the query: "{query}"')
                    case 'toolResult':
                        if msg['name'] == 'python_repl':
                            for result in msg['content']:
                                st.code(result['text'], language="python")
                        elif msg['name'] == 'search_datasets':
                            st.markdown(msg['content'])
                    case 'result':
                        result = msg['result']
                        st.markdown(result['answer'])
                        if 'visualization' in result and result['visualization']:
                            st.image(f"data:image/png;base64,{result['visualization']}")
                        if 'metrics' in result:
                            str_buffer = []
                            metrics = result['metrics']
                            if 'agent' in metrics:
                                agent_metrics = metrics['agent']
                                str_buffer.append("Agent Metrics:")
                                str_buffer.append(f" * ⏱️ Latency: {agent_metrics['total_duration']:.0f}s")
                                str_buffer.append(f" * 💸 On-Demand Cost: ${agent_metrics['on_demand_cost']:.2f}")
                            if 'query' in metrics:
                                query_metrics = metrics['query']
                                datasets = query_metrics['datasets']
                                if datasets:
                                    str_buffer.append("\nUsed Datasets:")
                                    for dataset in datasets:
                                        str_buffer.append(format_dataset_item(dataset))
                            st.markdown('\n'.join(str_buffer))


# Controls
with st.sidebar:
    st.header("Data Analyst Agent")
    st.markdown("This agent helps you analyse data from the UK Office for National Statistics (ONS) datasets.")
    st.markdown("""For example, you can ask questions like:
 * *Graph the employment rate through the years.*
 * *Did Brexit change trade with the EU?*
 * *When was the highest inflation rate in the UK?*
""")
    if user_prompt := st.chat_input("Enter your question here."):
        agent_chat(user_prompt)

    st.button("Logout", "logout_btn", on_click=lambda: authenticator.logout())
