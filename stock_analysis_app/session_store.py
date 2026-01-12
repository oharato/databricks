import json
import time
import streamlit as st
from streamlit_local_storage import LocalStorage

__all__ = ['init_session_state', 'update_ls', 'get_state_from_ls']

# localStorage コンポーネントの初期化
# Streamlitのコンポーネントはメインスクリプトで1回だけ呼ばれるのが望ましいため、
# ここで初期化するが、import時に実行される点に注意。
# localS = LocalStorage()
LS_KEY = "stock_app_user_data"

# デフォルトの銘柄リスト構造
DEFAULT_LISTS = {"Default": ["1301", "3031"]}
DEFAULT_STATE = {"lists": DEFAULT_LISTS, "current_list": "Default"}

def get_local_storage():
    return LocalStorage()

def get_state_from_ls():
    """localStorageから状態を取得する。取得できない場合はデフォルトまたはsession_stateを返す"""
    if "user_data_loaded" in st.session_state and st.session_state.user_data_loaded:
        return st.session_state.user_data

    ls_item = get_local_storage().getItem(LS_KEY)
    if ls_item:
        try:
            return json.loads(ls_item)
        except:
            return DEFAULT_STATE
    return DEFAULT_STATE

def save_state_to_ls(state):
    """状態をlocalStorageに保存する"""
    if st.session_state.get("first_run", False):
        return

    json_str = json.dumps(state)
    get_local_storage().setItem(LS_KEY, json_str, key=f"set_user_data_{int(time.time())}")

def update_ls():
    """session_stateの内容をlocalStorageに保存する"""
    save_state_to_ls(st.session_state.user_data)

def init_session_state():
    """セッション状態の初期化とロード処理"""
    if "is_started" not in st.session_state:
        st.session_state.is_started = True
        st.session_state.first_run = True
    else:
        st.session_state.first_run = False

    if "user_data" not in st.session_state:
        st.session_state.user_data = DEFAULT_STATE

    # ロード処理
    loaded_state = get_state_from_ls()
    if loaded_state != DEFAULT_STATE:
        if st.session_state.user_data == DEFAULT_STATE:
             st.session_state.user_data = loaded_state
             st.session_state.user_data_loaded = True
