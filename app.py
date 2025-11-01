import streamlit as st
from agent import create_agent_executor

st.set_page_config(
    page_title="Project Samarth",
    page_icon="🇮🇳",
    layout="wide"
)

st.title("🇮🇳 Project Samarth")
st.caption("An intelligent Q&A system for India's agricultural and climate data.")

if "agent_executor" not in st.session_state:
    with st.spinner("Initializing AI agent..."):
        st.session_state.agent_executor = create_agent_executor()

if "messages" not in st.session_state:
    st.session_state.messages = []
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

prompt = st.chat_input("Ask a question, e.g., 'Compare rainfall in Punjab and Vidarbha from 2010 to 2015'")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Analyzing data..."):
            
            response = st.session_state.agent_executor.invoke({
                "input": prompt,
                "chat_history": st.session_state.chat_history
            })
            
            ai_response = response['output']
            st.markdown(ai_response)
    
    st.session_state.messages.append({"role": "assistant", "content": ai_response})
    
    st.session_state.chat_history.append(("human", prompt))
    st.session_state.chat_history.append(("ai", ai_response))