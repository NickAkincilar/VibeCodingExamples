import streamlit as st
import requests
import snowflake.connector

MCP_DATABASE = st.secrets["mcp"]["database"]
MCP_SCHEMA = st.secrets["mcp"]["schema"]
MCP_SERVER_NAME = st.secrets["mcp"]["server_name"]
HOST = st.secrets["snowflake"]["host"]
MCP_ENDPOINT = f"/api/v2/databases/{MCP_DATABASE}/schemas/{MCP_SCHEMA}/mcp-servers/{MCP_SERVER_NAME}"

if "messages" not in st.session_state:
    st.session_state.messages = []
if "request_id" not in st.session_state:
    st.session_state.request_id = 0
if "auth_token" not in st.session_state:
    st.session_state.auth_token = None
if "auth_mode" not in st.session_state:
    st.session_state.auth_mode = None
if "tools" not in st.session_state:
    st.session_state.tools = []
if "selected_tool" not in st.session_state:
    st.session_state.selected_tool = None
if st.session_state.auth_token and not st.session_state.auth_mode:
    st.session_state.auth_token = None


def validate_token(token, mode):
    headers = {"Content-Type": "application/json"}
    if mode == "pat":
        headers["Authorization"] = f"Bearer {token}"
        headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN"
    elif mode == "oauth":
        headers["Authorization"] = f"Bearer {token}"
        headers["X-Snowflake-Authorization-Token-Type"] = "OAUTH"
    else:
        headers["Authorization"] = f'Snowflake Token="{token}"'
    payload = {"jsonrpc": "2.0", "id": 0, "method": "tools/list", "params": {}}
    try:
        resp = requests.post(
            url=f"https://{HOST}{MCP_ENDPOINT}",
            headers=headers,
            json=payload,
            timeout=15,
        )
        if resp.status_code == 200:
            return True, None
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, str(e)


def authenticate_pat():
    token = st.secrets["snowflake"]["pat"]
    ok, err = validate_token(token, "pat")
    if ok:
        st.session_state.auth_token = token
        st.session_state.auth_mode = "pat"
    else:
        st.sidebar.error(f"PAT authentication failed: {err}")


def authenticate_oauth(user, password, role):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=st.secrets["snowflake"]["account"],
            host=HOST,
            port=443,
            role=role,
        )
        st.session_state.auth_token = conn.rest.token
        st.session_state.auth_mode = "oauth"
        return True
    except Exception as e:
        st.error(f"Login failed: {e}")
        return False


def get_auth_headers():
    headers = {"Content-Type": "application/json"}
    if st.session_state.auth_mode == "pat":
        headers["Authorization"] = f"Bearer {st.session_state.auth_token}"
        headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN"
    elif st.session_state.auth_mode == "oauth":
        headers["Authorization"] = f"Bearer {st.session_state.auth_token}"
        headers["X-Snowflake-Authorization-Token-Type"] = "OAUTH"
    else:
        headers["Authorization"] = f'Snowflake Token="{st.session_state.auth_token}"'
    return headers


def fetch_tools():
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "tools/list",
        "params": {},
    }
    resp = requests.post(
        url=f"https://{HOST}{MCP_ENDPOINT}",
        headers=get_auth_headers(),
        json=payload,
        timeout=30,
    )
    if resp.status_code == 200:
        tools = resp.json().get("result", {}).get("tools", [])
        st.session_state.tools = tools
        if tools and not st.session_state.selected_tool:
            st.session_state.selected_tool = tools[0].get("name")
        return tools
    return []


def call_mcp_tool(message):
    st.session_state.request_id += 1
    payload = {
        "jsonrpc": "2.0",
        "id": st.session_state.request_id,
        "method": "tools/call",
        "params": {
            "name": st.session_state.selected_tool,
            "arguments": {
                "text": message,
            },
        },
    }

    resp = requests.post(
        url=f"https://{HOST}{MCP_ENDPOINT}",
        headers=get_auth_headers(),
        json=payload,
        timeout=120,
    )

    if resp.status_code != 200:
        return f"Error: {resp.status_code} - {resp.text}"

    data = resp.json()

    if "error" in data:
        return f"Error: {data['error'].get('message', 'Unknown error')}"

    result = data.get("result", {})
    content_items = result.get("content", [])
    texts = []
    for item in content_items:
        if item.get("type") == "text":
            texts.append(item.get("text", ""))
    return "\n".join(texts) if texts else "No response content."


def new_conversation():
    st.session_state.messages = []


def logout():
    st.session_state.auth_token = None
    st.session_state.auth_mode = None
    st.session_state.messages = []
    st.session_state.tools = []
    st.session_state.selected_tool = None


st.sidebar.title("MCP Server Tester")
st.sidebar.caption(f"Server: `{MCP_DATABASE}.{MCP_SCHEMA}.{MCP_SERVER_NAME}`")

if st.session_state.auth_token is None:
    st.sidebar.subheader("Authentication")
    auth_choice = st.sidebar.segmented_control(
        "Method", ["PAT", "OAuth Token", "Username / Password"], default="PAT"
    )

    if auth_choice == "PAT":
        if st.sidebar.button("Connect with PAT"):
            authenticate_pat()
            st.rerun()
    elif auth_choice == "OAuth Token":
        with st.sidebar.form("oauth_form"):
            token = st.text_input("OAuth access token", type="password")
            submitted = st.form_submit_button("Connect")
            if submitted and token:
                ok, err = validate_token(token, "oauth")
                if ok:
                    st.session_state.auth_token = token
                    st.session_state.auth_mode = "oauth"
                    st.rerun()
                else:
                    st.error(f"OAuth token rejected: {err}")
    else:
        with st.sidebar.form("login_form"):
            user = st.text_input("User")
            password = st.text_input("Password", type="password")
            role = st.text_input("Role", value="SYSADMIN")
            submitted = st.form_submit_button("Connect")
            if submitted:
                if authenticate_oauth(user, password, role):
                    st.rerun()

    st.stop()

st.sidebar.success(f"Authenticated via {st.session_state.auth_mode.upper()}")
st.sidebar.button("New conversation", on_click=new_conversation)
st.sidebar.button("Logout", on_click=logout)

with st.sidebar.expander("Available tools", expanded=True):
    tools = fetch_tools()
    if tools:
        tool_names = [t.get("name") for t in tools]
        idx = tool_names.index(st.session_state.selected_tool) if st.session_state.selected_tool in tool_names else 0
        selected = st.radio("Select tool", tool_names, index=idx, label_visibility="collapsed")
        st.session_state.selected_tool = selected
        for tool in tools:
            if tool.get("name") == selected:
                st.caption(tool.get("description", ""))
                break
    else:
        st.write("No tools found or unable to list.")

st.title("MCP Server Tester")
tool_label = st.session_state.selected_tool or "no tool"
st.caption(f"`{MCP_DATABASE}.{MCP_SCHEMA}.{MCP_SERVER_NAME}` / tool `{tool_label}`")

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ask a question..."):
    if not st.session_state.selected_tool:
        st.error("No tool selected. Check the sidebar.")
    else:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Calling MCP server..."):
                response = call_mcp_tool(prompt)
            st.write(response)

        st.session_state.messages.append({"role": "assistant", "content": response})
