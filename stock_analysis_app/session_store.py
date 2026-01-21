import json
import streamlit as st
import streamlit.components.v1 as components

__all__ = ['init_session_state', 'update_ls']

LS_KEY = "stock_app_user_data"

# デフォルトの銘柄リスト構造
DEFAULT_LISTS = {"Default": ["1301", "3031"]}
DEFAULT_STATE = {"lists": DEFAULT_LISTS, "current_list": "Default"}

def load_from_browser_storage():
    """ブラウザのlocalStorageからデータを読み込む"""
    html_code = f"""
    <script>
        const data = localStorage.getItem('{LS_KEY}');
        const result = data ? JSON.parse(data) : null;
        window.parent.postMessage({{
            type: 'streamlit:setComponentValue',
            value: result
        }}, '*');
    </script>
    """
    result = components.html(html_code, height=0)
    return result if result else None

def save_to_browser_storage(state):
    """ブラウザのlocalStorageにデータを保存する"""
    json_str = json.dumps(state).replace("'", "\\'")
    html_code = f"""
    <script>
        localStorage.setItem('{LS_KEY}', '{json_str}');
    </script>
    """
    components.html(html_code, height=0)

def update_ls():
    """session_stateの内容をブラウザのlocalStorageに保存する"""
    if not st.session_state.get("first_run", False):
        save_to_browser_storage(st.session_state.user_data)

def init_session_state():
    """セッション状態の初期化とロード処理"""
    # 初回実行フラグの設定
    if "is_started" not in st.session_state:
        st.session_state.is_started = True
        st.session_state.first_run = True
    else:
        st.session_state.first_run = False

    # user_dataの初期化（まず必ずDEFAULT_STATEで初期化）
    if "user_data" not in st.session_state:
        st.session_state.user_data = DEFAULT_STATE.copy()
        st.session_state.user_data_loaded = False

    # 初回のみブラウザのlocalStorageから読み込み
    if not st.session_state.user_data_loaded:
        loaded_state = load_from_browser_storage()
        if loaded_state and isinstance(loaded_state, dict) and "lists" in loaded_state:
            st.session_state.user_data = loaded_state
            st.session_state.user_data_loaded = True
        else:
            # 読み込み失敗または初回アクセス時はデフォルトを維持
            st.session_state.user_data_loaded = True
