import copy
import streamlit as st

__all__ = ['init_session_state']

# デフォルトの銘柄リスト構造
DEFAULT_LISTS = {"Default": ["1301", "3031"]}
DEFAULT_STATE = {"lists": DEFAULT_LISTS, "current_list": "Default"}

def init_session_state():
    """セッション状態の初期化"""
    if "is_started" not in st.session_state:
        st.session_state.is_started = True
        st.session_state.first_run = True
    else:
        st.session_state.first_run = False

    # user_dataの初期化（まず必ずDEFAULT_STATEで初期化）
    if "user_data" not in st.session_state:
        st.session_state.user_data = copy.deepcopy(DEFAULT_STATE)
