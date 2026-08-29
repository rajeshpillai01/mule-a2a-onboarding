"""
Streamlit UI for the A2A MuleSoft Onboarding Workbench.
Connects to the Orchestrator Agent at http://localhost:8100 via A2A SSE.
"""
import asyncio
import io
import json
import os
import zipfile

import httpx
import streamlit as st

st.set_page_config(
    page_title="MuleSoft A2A Onboarding Workbench",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.main { background-color: #f8fafc; }
h1 { color: #0066cc; font-family: 'Helvetica Neue', Arial, sans-serif; font-weight: 700; }
h2, h3 { color: #1e293b; font-family: 'Helvetica Neue', Arial, sans-serif; }
.stButton>button { background-color: #0066cc; color: white !important; border-radius: 6px;
                   font-weight: 600; transition: all 0.3s; }
.stButton>button:hover { background-color: #004d99; }
.agent-badge { display: inline-block; background: #e8f4fd; border: 1.5px solid #0066cc;
               border-radius: 6px; padding: 4px 12px; font-weight: 600; color: #0066cc;
               font-size: 0.85rem; margin-bottom: 6px; }
</style>
""", unsafe_allow_html=True)

ORCHESTRATOR = "http://localhost:8100"
WORKSPACE    = os.path.dirname(os.path.abspath(__file__))
ISAG_PATH    = os.path.join(WORKSPACE, "isag_document.json")
DS_PATH      = os.path.join(WORKSPACE, "technical_design_spec.json")
AGENTS_MD    = os.path.join(WORKSPACE, "AGENTS.md")
MULE_DIR     = os.path.join(WORKSPACE, "mule-project")

AGENT_META = {
    "architect": {"name": "Architect Agent",  "emoji": "🏗"},
    "dev_lead":  {"name": "Developer Lead",   "emoji": "📋"},
    "developer": {"name": "Developer (Ona)",  "emoji": "💡"},
    "ona":       {"name": "Ona Agent",        "emoji": "🤖"},
    "orchestrator": {"name": "Orchestrator",  "emoji": "🎯"},
}


async def _collect_events(url: str, req_body: dict) -> list:
    """Collect all SSE events from a streaming endpoint into a list."""
    events = []
    async with httpx.AsyncClient(timeout=600) as client:
        async with client.stream(
            "POST", url, json=req_body,
            headers={"Accept": "text/event-stream"},
        ) as resp:
            resp.raise_for_status()
            buffer = ""
            async for chunk in resp.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    event_str, buffer = buffer.split("\n\n", 1)
                    for line in event_str.splitlines():
                        if line.startswith("data:"):
                            raw = line[5:].strip()
                            if raw and raw != "[DONE]":
                                try:
                                    events.append(json.loads(raw))
                                except Exception:
                                    pass
    return events


# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("A2A Onboarding Studio")
    st.markdown("Powered by the **Google A2A protocol** — each agent is an independent HTTP microservice.")
    st.markdown("---")
    token = st.text_input("Anthropic / Build-CLI Token:", type="password",
                          help="Paste your token here or set ANTHROPIC_API_KEY before launching agents.")
    if token:
        os.environ["ANTHROPIC_AUTH_TOKEN"] = token.strip()

    anypoint_token = st.text_input("Anypoint Platform Token (optional):", type="password")
    anypoint_org   = st.text_input("Anypoint Org ID (optional):")

    st.markdown("---")
    st.markdown("**Agent ports**")
    for name, port in [("Orchestrator", 8100), ("Architect", 8101),
                       ("Dev Lead", 8102), ("Developer", 8103),
                       ("Ona", 8104), ("Registry", 8105)]:
        st.markdown(f"`:{port}` {name}")

    if st.button("Check Agent Health"):
        for name, port in [("Orchestrator", 8100), ("Architect", 8101),
                           ("Dev Lead", 8102), ("Developer", 8103),
                           ("Ona", 8104), ("Registry", 8105)]:
            try:
                r = httpx.get(f"http://localhost:{port}/health", timeout=2)
                st.success(f"✅ {name}")
            except Exception:
                st.error(f"❌ {name} — not reachable")

# ── Header ───────────────────────────────────────────────────────────────────
st.title("🏢 MuleSoft A2A Onboarding Workbench")
st.markdown(
    "Multi-agent AI pipeline built on the **Google A2A open protocol**. "
    "Each agent is a standalone HTTP microservice — discoverable, composable, and interoperable."
)
st.markdown("---")

tab_pipeline, tab_generate, tab_registry, tab_skills = st.tabs([
    "📂 Onboarding Pipeline",
    "⚙️ Generate Mule 4 Project",
    "🗺️ Agent Registry",
    "🛠️ Skill Desk",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — PIPELINE
# ════════════════════════════════════════════════════════════════════════════
with tab_pipeline:
    st.markdown("### Step 1: Upload Documents")
    c1, c2 = st.columns(2)
    with c1:
        uploaded_fsd = st.file_uploader("Functional Specification Document:", type=["txt", "md"])
    with c2:
        uploaded_csv = st.file_uploader("Field Mapping CSV:", type=["csv"])

    st.markdown("---")
    st.markdown("### Step 2: Run the Agentic Pipeline")
    st.caption(
        "The Orchestrator Agent coordinates Architect → Developer Lead → Developer. "
        "Agents may pause for clarification before generating their output — all dialogue is observable in real time."
    )

    # Architecture diagram
    st.markdown("""
```
Streamlit  ──SSE──►  Orchestrator :8100
                          ├──────────────►  Architect Agent   :8101
                          │                 (clarification ↕)
                          ├──────────────►  Dev Lead Agent    :8102
                          │                 (clarification ↕)
                          └──────────────►  Developer Agent   :8103
```
""")

    if st.button("🚀 Run A2A Onboarding Pipeline"):
        if not uploaded_fsd or not uploaded_csv:
            st.error("Please upload both the FSD and the Mapping CSV.")
        else:
            fsd_text = uploaded_fsd.read().decode("utf-8")
            csv_text = uploaded_csv.read().decode("utf-8")

            payload = {
                "fsd": fsd_text,
                "csv": csv_text,
                "anypoint_token": anypoint_token or "",
                "anypoint_org_id": anypoint_org or "",
            }

            st.markdown("---")
            st.markdown("### Live Agent Conversation")

            step_placeholders: dict = {}
            step_char_counts:  dict = {}
            answer_ph   = None
            answer_text = ""
            dialogue_shown: set = set()

            try:
                events = asyncio.run(
                    _collect_events(f"{ORCHESTRATOR}/tasks/sendSubscribe", req_body={
                        "id": __import__("uuid").uuid4().__str__(),
                        "message": {"role": "user", "parts": [{"type": "data", "data": payload}]},
                        "skillId": "run-onboarding-pipeline",
                    })
                )
            except Exception as e:
                st.error(f"Pipeline error: {e}")
                events = []

            pipeline_succeeded = False
            for ev in events:
                ev_type = ev.get("type", "")

                if ev_type == "error":
                    st.error(f"Pipeline error: {ev.get('message', 'Unknown error')}")
                    continue

                if ev_type != "pipeline_event":
                    continue

                event_type = ev.get("event_type", "")
                agent      = ev.get("agent", "")
                content    = ev.get("content", "")
                target     = ev.get("target", "")
                meta       = AGENT_META.get(agent, {"name": agent, "emoji": "🤖"})
                emoji      = meta["emoji"]
                name       = meta["name"]

                if event_type == "step_start":
                    st.markdown("---")
                    with st.chat_message(emoji):
                        st.markdown(f"**{name}**")
                        st.caption(content)
                        step_placeholders[agent] = st.empty()
                        step_char_counts[agent]  = 0
                    step_placeholders[agent].info("Starting...")

                elif event_type == "step_chunk":
                    step_char_counts[agent] = step_char_counts.get(agent, 0) + len(content)
                    if agent in step_placeholders:
                        step_placeholders[agent].info(
                            f"Generating... **{step_char_counts[agent]:,}** chars"
                        )

                elif event_type == "step_done":
                    if content.startswith("Error:"):
                        if agent in step_placeholders:
                            step_placeholders[agent].error(content)
                    else:
                        if agent in step_placeholders:
                            step_placeholders[agent].success("Complete")
                        if agent == "developer":
                            pipeline_succeeded = True

                elif event_type == "dialogue_question":
                    phase_key = f"{agent}_dialogue"
                    if target == "__section__":
                        if phase_key not in dialogue_shown:
                            st.markdown("---")
                            st.markdown("#### Agent Dialogue (A2A clarification gate)")
                            dialogue_shown.add(phase_key)
                        continue
                    t_meta = AGENT_META.get(target, {"name": target, "emoji": "🤖"})
                    with st.chat_message(emoji):
                        st.markdown(f"**{name}** to **{t_meta['name']}**\n\n> {content}")

                elif event_type == "dialogue_answer_start":
                    t_meta = AGENT_META.get(target, {"name": target})
                    with st.chat_message(emoji):
                        st.markdown(f"**{name}** replying to **{t_meta['name']}**")
                        answer_ph   = st.empty()
                        answer_text = ""

                elif event_type == "dialogue_answer_chunk":
                    answer_text += content
                    if answer_ph:
                        answer_ph.markdown(answer_text)

                elif event_type == "dialogue_answer_done":
                    answer_ph   = None
                    answer_text = ""

            st.markdown("---")
            if pipeline_succeeded:
                st.balloons()
                st.success("A2A Pipeline complete! AGENTS.md is ready.")
            else:
                st.warning("Pipeline did not complete all steps. Check the errors above and verify your API key is set in the launch.py terminal.")

    # ── Artifact previews ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Pipeline Artifacts")
    p1, p2, p3 = st.tabs(["🔬 ISAG", "📄 TDS", "🤖 AGENTS.md"])

    with p1:
        if os.path.exists(ISAG_PATH):
            with open(ISAG_PATH) as f:
                st.json(f.read())
        else:
            st.info("Run the pipeline to generate the ISAG.")

    with p2:
        if os.path.exists(DS_PATH):
            with open(DS_PATH) as f:
                st.json(f.read())
        else:
            st.info("Run the pipeline to generate the TDS.")

    with p3:
        if os.path.exists(AGENTS_MD):
            with open(AGENTS_MD) as f:
                st.code(f.read(), language="markdown")
        else:
            st.info("Run the pipeline to generate AGENTS.md.")


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — MULE 4 GENERATION
# ════════════════════════════════════════════════════════════════════════════
with tab_generate:
    st.markdown("### Generate Mule 4 Project via Ona Agent")
    st.caption(
        "Ona reads AGENTS.md and streams every project file to disk. "
        "File events appear in real time as they are written."
    )

    agents_md_exists = os.path.exists(AGENTS_MD)
    if not agents_md_exists:
        st.info("⏳ Run the pipeline first to generate AGENTS.md.")
    else:
        with open(AGENTS_MD) as f:
            agents_md_content = f.read()

        csv_content = ""
        csv_path    = os.path.join(WORKSPACE, "mapping.csv")
        if os.path.exists(csv_path):
            with open(csv_path) as f:
                csv_content = f.read()

        if st.button("🚀 Run Ona — Generate Mule 4 Code"):
            payload = {
                "agents_md":      agents_md_content,
                "csv_content":    csv_content,
                "anypoint_token": anypoint_token or "",
                "project_dir":    MULE_DIR,
            }

            file_records: list = []

            try:
                with st.status("🤖 Ona — Generating Mule 4 project...", expanded=True) as ona_status:
                    progress = st.empty()
                    file_log = st.container()

                    req_body = {
                        "id": __import__("uuid").uuid4().__str__(),
                        "message": {"role": "user", "parts": [{"type": "data", "data": payload}]},
                        "skillId": "run-mule-generation",
                    }
                    events = asyncio.run(_collect_events(
                        f"{ORCHESTRATOR}/tasks/sendSubscribe", req_body=req_body
                    ))

                    char_count = 0
                    for ev in events:
                        t = ev.get("type")
                        if t == "pipeline_event" and ev.get("event_type") == "step_chunk":
                            try:
                                char_count = int(ev.get("content", 0))
                            except Exception:
                                char_count += len(ev.get("content", ""))
                            progress.info(f"⠴ Generating... **{char_count:,}** chars")
                        elif t == "file_creating":
                            with file_log:
                                st.markdown(f"📝 `{ev.get('path')}`")
                        elif t == "file_created":
                            p = ev.get("path", "")
                            s = ev.get("size",  0)
                            file_records.append((p, s))
                            with file_log:
                                st.success(f"✅ `{p}` — **{s:,}** bytes")
                        elif t == "skill_prep_done":
                            with file_log:
                                st.success(f"✅ {ev.get('content', '')}")
                        elif t == "skill_prep_skip":
                            with file_log:
                                st.warning(f"⚠️ {ev.get('content', '')}")

                    progress.empty()
                    ona_status.update(
                        label=f"✅ Ona — {len(file_records)} files generated",
                        state="complete", expanded=False,
                    )

                if file_records:
                    st.markdown("### 📁 Generated Mule 4 Project")
                    lang_map = {"xml": "xml", "raml": "yaml", "yaml": "yaml",
                                "json": "json", "properties": "properties", "md": "markdown"}
                    for rel_path, size in sorted(file_records):
                        ext       = rel_path.rsplit(".", 1)[-1] if "." in rel_path else "text"
                        full_path = os.path.join(MULE_DIR, rel_path)
                        with st.expander(f"📄 `{rel_path}`  ·  {size:,} bytes"):
                            if os.path.exists(full_path):
                                with open(full_path, errors="replace") as fh:
                                    st.code(fh.read(), language=lang_map.get(ext, "text"))

                    zip_buf = io.BytesIO()
                    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                        for rel_path, _ in file_records:
                            fp = os.path.join(MULE_DIR, rel_path)
                            if os.path.exists(fp):
                                zf.write(fp, rel_path)
                    zip_buf.seek(0)
                    st.download_button(
                        "⬇️ Download Mule 4 Project (.zip)",
                        data=zip_buf,
                        file_name="mule-project.zip",
                        mime="application/zip",
                    )
                    st.balloons()

            except Exception as e:
                st.error(f"Code generation failed: {e}")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — AGENT REGISTRY
# ════════════════════════════════════════════════════════════════════════════
with tab_registry:
    st.markdown("### A2A Agent Registry")
    st.caption("All agents self-register on startup. The registry exposes their Agent Cards for discovery.")

    if st.button("🔄 Refresh Registry"):
        try:
            r = httpx.get("http://localhost:8105/agents", timeout=5)
            agents = r.json()
            for card in agents:
                with st.expander(f"**{card.get('name')}**  —  {card.get('url')}"):
                    st.markdown(f"*{card.get('description')}*")
                    st.markdown(f"**Version:** {card.get('version')} | "
                                f"**Streaming:** {card.get('capabilities', {}).get('streaming')}")
                    st.markdown("**Skills:**")
                    for skill in card.get("skills", []):
                        st.markdown(
                            f"- `{skill['id']}` — **{skill['name']}**: {skill['description']}"
                        )
                    st.markdown("**Full Agent Card (JSON):**")
                    st.json(card)
        except Exception as e:
            st.error(f"Registry not reachable: {e}. Ensure launch.py is running.")


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — SKILL DESK
# ════════════════════════════════════════════════════════════════════════════
with tab_skills:
    st.markdown("### Ad-hoc Skill Invocation")
    st.caption("Call any agent skill directly without running the full pipeline.")

    skill_agent_map = {
        "Architect — Generate ISAG":          (ORCHESTRATOR,           "generate-isag"),
        "Developer Lead — Generate TDS":       ("http://localhost:8102", "generate-tds"),
        "Developer — Generate AGENTS.md":      ("http://localhost:8103", "generate-agents-md"),
    }

    choice = st.selectbox("Select skill:", list(skill_agent_map.keys()))
    agent_url, skill_id = skill_agent_map[choice]

    if "ISAG" in choice:
        fsd_in = st.text_area("FSD text:", height=150)
        csv_in = st.text_area("CSV content:", height=100)
        skill_payload = {"fsd": fsd_in, "csv": csv_in}
    elif "TDS" in choice:
        isag_in = st.text_area("ISAG JSON:", height=150)
        skill_payload = {"isag_json": isag_in}
    else:
        tds_in = st.text_area("TDS JSON:", height=150)
        skill_payload = {"tds_json": tds_in}

    if st.button(f"⚡ Execute: {choice}"):
        with st.spinner("Calling agent..."):
            try:
                req_body = {
                    "id": __import__("uuid").uuid4().__str__(),
                    "message": {"role": "user", "parts": [{"type": "data", "data": skill_payload}]},
                    "skillId": skill_id,
                }
                events = asyncio.run(_collect_events(f"{agent_url}/tasks/sendSubscribe", req_body))
                output = ""
                for ev in events:
                    if ev.get("type") == "chunk":
                        output += ev.get("text", "")
                st.markdown("#### Output")
                st.code(output or "(no output)", language="json")
            except Exception as e:
                st.error(f"Skill invocation failed: {e}")

