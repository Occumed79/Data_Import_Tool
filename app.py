from __future__ import annotations

import json

import duckdb
import polars as pl
import streamlit as st

from src.database import (
    database_available,
    list_history,
    list_recipes,
    rollback_import,
    save_recipe,
    write_frame,
)
from src.exports import to_csv_bytes, to_excel_bytes, to_json_bytes, to_parquet_bytes
from src.intake import load_bytes, load_url
from src.transform import apply_transform, profile_frame


st.set_page_config(
    page_title="Occu-Med Data Import Tool",
    page_icon="↗",
    layout="wide",
)

st.markdown(
    """
    <style>
      .stApp { background: #f6f7f9; color: #17191c; }
      .block-container { max-width: 1500px; padding-top: 2.2rem; padding-bottom: 4rem; }
      h1, h2, h3 { letter-spacing: -0.035em; }
      [data-testid="stMetric"] {
        background: white;
        border: 1px solid #e6e8ec;
        border-radius: 18px;
        padding: 16px 18px;
      }
      [data-testid="stDataFrame"] {
        background: white;
        border: 1px solid #e6e8ec;
        border-radius: 16px;
        overflow: hidden;
      }
      .dit-kicker {
        color: #3976d9;
        font-size: .78rem;
        font-weight: 700;
        letter-spacing: .12em;
      }
      .dit-subtle { color: #68707b; }
    </style>
    """,
    unsafe_allow_html=True,
)

if "datasets" not in st.session_state:
    st.session_state.datasets = {}
if "working" not in st.session_state:
    st.session_state.working = {}
if "configs" not in st.session_state:
    st.session_state.configs = {}
if "local_recipes" not in st.session_state:
    st.session_state.local_recipes = {}

st.markdown('<div class="dit-kicker">OCCU-MED</div>', unsafe_allow_html=True)
st.title("Data Import Workbench")
st.markdown(
    '<div class="dit-subtle">Bring messy source files in once, clean them deliberately, and hand downstream apps consistent data.</div>',
    unsafe_allow_html=True,
)

tab_import, tab_clean, tab_sql, tab_export, tab_recipes = st.tabs(
    ["Import", "Clean & Transform", "SQL Workbench", "Export & Neon", "Recipes & History"]
)


def register_frames(frames: dict[str, pl.DataFrame]) -> None:
    for name, frame in frames.items():
        st.session_state.datasets[name] = frame
        st.session_state.working[name] = frame
        st.session_state.configs[name] = {}


def selected_dataset(key: str) -> str | None:
    names = list(st.session_state.working.keys())
    if not names:
        return None
    return st.selectbox("Dataset", names, key=key)


def frame_metrics(df: pl.DataFrame) -> None:
    a, b, c, d = st.columns(4)
    a.metric("Rows", f"{df.height:,}")
    b.metric("Columns", f"{df.width:,}")
    text_cols = sum(1 for dtype in df.dtypes if dtype == pl.String)
    c.metric("Text columns", f"{text_cols:,}")
    nulls = sum(df.null_count().row(0)) if df.width else 0
    d.metric("Null cells", f"{nulls:,}")


with tab_import:
    st.subheader("Import source data")
    left, right = st.columns([1.2, 1])

    with left:
        uploads = st.file_uploader(
            "Upload files",
            type=["csv", "tsv", "txt", "xlsx", "xls", "json", "jsonl", "ndjson", "parquet", "zip"],
            accept_multiple_files=True,
            help="ZIP files can contain multiple supported files.",
        )
        if st.button("Load uploaded files", type="primary", disabled=not uploads):
            loaded = 0
            for uploaded in uploads or []:
                try:
                    frames = load_bytes(uploaded.name, uploaded.getvalue())
                    register_frames(frames)
                    loaded += len(frames)
                except Exception as exc:
                    st.error(f"{uploaded.name}: {exc}")
            if loaded:
                st.success(f"Loaded {loaded} dataset(s).")

    with right:
        url = st.text_input("Or pull from a direct file URL", placeholder="https://.../data.csv")
        if st.button("Load URL", disabled=not url):
            try:
                frames = load_url(url)
                register_frames(frames)
                st.success(f"Loaded {len(frames)} dataset(s) from URL.")
            except Exception as exc:
                st.error(str(exc))

    if st.session_state.working:
        name = selected_dataset("import_dataset")
        df = st.session_state.working[name]
        frame_metrics(df)
        st.markdown("#### Preview")
        st.dataframe(df.head(100).to_pandas(), use_container_width=True, hide_index=True)
        st.markdown("#### Column profile")
        st.dataframe(profile_frame(df), use_container_width=True, hide_index=True)


with tab_clean:
    st.subheader("Clean & transform")
    name = selected_dataset("clean_dataset")

    if not name:
        st.info("Import a dataset first.")
    else:
        source_df = st.session_state.datasets[name]
        current_df = st.session_state.working[name]

        with st.form("transform_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                standardize = st.checkbox("Standardize column names", value=True)
                trim = st.checkbox("Trim text / normalize blanks", value=True)
                remove_blank = st.checkbox("Remove fully blank rows", value=True)
            with c2:
                drop_columns = st.multiselect("Drop columns", current_df.columns)
                dedupe_columns = st.multiselect("De-duplicate using", current_df.columns)
            with c3:
                titlecase_columns = st.multiselect("Title-case columns", current_df.columns)
                phone_column = st.selectbox("Normalize US phone column", [""] + current_df.columns)
                provider_source = st.selectbox(
                    "Classify provider type from",
                    [""] + current_df.columns,
                    help="Optional heuristic classification from a name, type, or description field.",
                )

            st.markdown("##### Rename columns")
            rename_map = {}
            rename_cols = st.multiselect("Columns to rename", current_df.columns)
            if rename_cols:
                ren_cols = st.columns(min(3, len(rename_cols)))
                for i, col in enumerate(rename_cols):
                    with ren_cols[i % len(ren_cols)]:
                        rename_map[col] = st.text_input(
                            col,
                            value=col,
                            key=f"rename_{name}_{col}",
                        )

            applied = st.form_submit_button("Apply transformation", type="primary")

        if applied:
            config = {
                "standardize_columns": standardize,
                "trim_strings": trim,
                "remove_blank_rows": remove_blank,
                "drop_columns": drop_columns,
                "dedupe_columns": dedupe_columns,
                "titlecase_columns": titlecase_columns,
                "phone_column": phone_column or None,
                "provider_type_source": provider_source or None,
                "rename_map": rename_map,
            }
            try:
                transformed = apply_transform(source_df, config)
                st.session_state.working[name] = transformed
                st.session_state.configs[name] = config
                st.success(f"Applied. {source_df.height:,} → {transformed.height:,} rows.")
            except Exception as exc:
                st.error(f"Transformation failed: {exc}")

        current_df = st.session_state.working[name]
        frame_metrics(current_df)
        st.dataframe(current_df.head(100).to_pandas(), use_container_width=True, hide_index=True)

        if st.button("Reset dataset"):
            st.session_state.working[name] = source_df
            st.session_state.configs[name] = {}
            st.rerun()


with tab_sql:
    st.subheader("DuckDB SQL Workbench")
    name = selected_dataset("sql_dataset")

    if not name:
        st.info("Import a dataset first.")
    else:
        df = st.session_state.working[name]
        st.caption('The current dataset is registered as "data". Read-only SELECT / WITH / DESCRIBE queries are allowed.')
        sql = st.text_area(
            "SQL",
            value="SELECT * FROM data LIMIT 100",
            height=180,
        )
        if st.button("Run query", type="primary"):
            normalized = sql.strip().lower()
            if not normalized.startswith(("select", "with", "describe", "show", "summarize", "explain")):
                st.error("Only read-only query statements are allowed here.")
            else:
                try:
                    con = duckdb.connect(database=":memory:")
                    con.register("data", df.to_arrow())
                    result = con.execute(sql).pl()
                    st.success(f"{result.height:,} row(s) returned.")
                    st.dataframe(result.to_pandas(), use_container_width=True, hide_index=True)
                    st.session_state["last_sql_result"] = result
                except Exception as exc:
                    st.error(str(exc))


with tab_export:
    st.subheader("Export & Neon")
    name = selected_dataset("export_dataset")

    if not name:
        st.info("Import a dataset first.")
    else:
        df = st.session_state.working[name]
        frame_metrics(df)

        st.markdown("#### Download")
        b1, b2, b3, b4 = st.columns(4)
        b1.download_button(
            "CSV",
            data=to_csv_bytes(df),
            file_name=f"{name}_clean.csv",
            mime="text/csv",
            use_container_width=True,
        )
        b2.download_button(
            "Parquet",
            data=to_parquet_bytes(df),
            file_name=f"{name}_clean.parquet",
            mime="application/octet-stream",
            use_container_width=True,
        )
        b3.download_button(
            "JSON",
            data=to_json_bytes(df),
            file_name=f"{name}_clean.json",
            mime="application/json",
            use_container_width=True,
        )
        b4.download_button(
            "Excel",
            data=to_excel_bytes(df),
            file_name=f"{name}_clean.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        st.markdown("#### Write to Neon / Postgres")
        if database_available():
            t1, t2 = st.columns([2, 1])
            table_name = t1.text_input("Target table", value=name)
            mode = t2.selectbox("Write mode", ["append", "replace"])
            if mode == "replace":
                st.caption("Replace snapshots an existing target table first, so it can be rolled back.")
            if st.button("Write to database", type="primary"):
                try:
                    result = write_frame(df, table_name, source_name=name, mode=mode)
                    msg = f"Wrote {result['rows']:,} rows to {result['table']}."
                    if result["snapshot_table"]:
                        msg += f" Snapshot: {result['snapshot_table']}."
                    st.success(msg)
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.info('Set DATABASE_URL to enable persistent recipes, direct Neon loading, history, and rollback.')


with tab_recipes:
    st.subheader("Recipes & history")
    name = selected_dataset("recipe_dataset")

    if name:
        config = st.session_state.configs.get(name) or {}
        st.markdown("#### Save current transform recipe")
        recipe_name = st.text_input("Recipe name", placeholder="Blue Hive Provider Import")
        if st.button("Save recipe", disabled=not recipe_name or not config):
            st.session_state.local_recipes[recipe_name.strip()] = config
            if database_available():
                try:
                    save_recipe(recipe_name.strip(), config)
                    st.success("Recipe saved to Neon.")
                except Exception as exc:
                    st.warning(f"Saved for this session, but Neon save failed: {exc}")
            else:
                st.success("Recipe saved for this session.")

    recipes = dict(st.session_state.local_recipes)
    if database_available():
        try:
            for row in list_recipes():
                recipes[row["name"]] = row["config"]
        except Exception as exc:
            st.warning(f"Could not load Neon recipes: {exc}")

    if recipes:
        st.markdown("#### Apply a saved recipe")
        chosen = st.selectbox("Saved recipe", list(recipes.keys()))
        st.code(json.dumps(recipes[chosen], indent=2), language="json")
        if name and st.button("Apply saved recipe"):
            try:
                transformed = apply_transform(st.session_state.datasets[name], recipes[chosen])
                st.session_state.working[name] = transformed
                st.session_state.configs[name] = recipes[chosen]
                st.success(f"Applied '{chosen}' to {name}.")
            except Exception as exc:
                st.error(str(exc))
    else:
        st.caption("No saved recipes yet.")

    if database_available():
        st.markdown("#### Database import history")
        try:
            history = list_history()
            if history:
                st.dataframe(history, use_container_width=True, hide_index=True)
                rollbackable = [row for row in history if row.get("snapshot_table")]
                if rollbackable:
                    labels = {
                        f"#{row['id']} — {row['target_table']} — {row['created_at']}": row["id"]
                        for row in rollbackable
                    }
                    choice = st.selectbox("Rollback a replace import", list(labels.keys()))
                    if st.button("Restore selected snapshot"):
                        try:
                            result = rollback_import(labels[choice])
                            st.success(
                                f"Restored {result['restored_table']} from {result['from_snapshot']}."
                            )
                        except Exception as exc:
                            st.error(str(exc))
            else:
                st.caption("No database imports yet.")
        except Exception as exc:
            st.warning(f"Could not load import history: {exc}")
