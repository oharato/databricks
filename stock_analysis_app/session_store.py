import json
import streamlit as st
from streamlit_javascript import st_javascript

__all__ = ['init_session_state', 'update_ls']

COOKIE_KEY = "stock_app_user_data"

# デフォルトの銘柄リスト構造
DEFAULT_LISTS = {"Default": ["1301", "3031"]}
DEFAULT_STATE = {"lists": DEFAULT_LISTS, "current_list": "Default"}

def load_from_browser_cookie():
    """ブラウザのcookieからデータを読み込む"""
    js_code = f"""
    function getCookie(name) {{
        const value = `; ${{document.cookie}}`;
        const parts = value.split(`; ${{name}}=`);
        if (parts.length === 2) return parts.pop().split(';').shift();
        return null;
    }}
    const data = getCookie('{COOKIE_KEY}');
    return data ? JSON.parse(decodeURIComponent(data)) : null;
    """
    result = st_javascript(js_code)
    return result if result else None

def save_to_browser_cookie(state):
    """ブラウザのcookieにデータを保存する（365日有効）"""
    json_str = json.dumps(state)
    js_code = f"""
    const data = {json.dumps(json_str)};
    const expires = new Date();
    expires.setTime(expires.getTime() + (365 * 24 * 60 * 60 * 1000)); // 365日
    document.cookie = '{COOKIE_KEY}=' + encodeURIComponent(data) + '; expires=' + expires.toUTCString() + '; path=/; SameSite=Lax';
    return true;
    """
    st_javascript(js_code)

def update_ls():
    """session_stateの内容をブラウザのcookieに保存する"""
    if not st.session_state.get("first_run", False):
        save_to_browser_cookie(st.session_state.user_data)

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

    # 初回のみブラウザのcookieから読み込み
    if not st.session_state.user_data_loaded:
        loaded_state = load_from_browser_cookie()
        if loaded_state and isinstance(loaded_state, dict) and "lists" in loaded_state:
            st.session_state.user_data = loaded_state
            st.session_state.user_data_loaded = True
        else:
            # 読み込み失敗または初回アクセス時はデフォルトを維持
            st.session_state.user_data_loaded = True
