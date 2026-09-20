from __future__ import annotations

import json

import duckdb
import polars as pl
import streamlit as st

from src.database import (
    database_available,
    list_history,
    list_merge_recipes,
    list_recipes,
    list_sources,
    rollback_import,
    save_merge_recipe,
    save_recipe,
    save_source,
    write_frame,
)
from src.exports import (
    to_csv_bytes,
    to_excel_bytes,
    to_json_bytes,
    to_parquet_bytes,
    to_validation_zip,
)
from src.intake import load_bytes, load_url
from src.geocode import geocode_dataframe
from src.normalize import add_address_key, find_fuzzy_duplicates
from src.quality import diff_frames, merge_frames, quarantine_required
from src.sources import refresh_all_active_sources, refresh_source
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

tab_import, tab_clean, tab_merge, tab_match, tab_sql, tab_export, tab_sources, tab_recipes = st.tabs(
    [
        "Import",
        "Clean & Transform",
        "Merge & Compare",
        "Match & Geocode",
        "SQL Workbench",
        "Export & Neon",
        "Sources & Refresh",
        "Recipes & History",
    ]
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

        st.markdown("#### Required-field validation / quarantine")
        required_columns = st.multiselect(
            "Fields that may not be blank",
            current_df.columns,
            key=f"required_fields_{name}",
        )
        if st.button("Validate and quarantine bad rows", disabled=not required_columns):
            try:
                clean_rows, bad_rows = quarantine_required(current_df, required_columns)
                st.session_state.working[name] = clean_rows
                if bad_rows.height:
                    quarantine_name = f"{name}__quarantine"
                    register_frames({quarantine_name: bad_rows})
                    st.warning(
                        f"Quarantined {bad_rows.height:,} row(s) into {quarantine_name}. "
                        f"{clean_rows.height:,} valid row(s) remain in {name}."
                    )
                else:
                    st.success("No rows failed the required-field rules.")
            except Exception as exc:
                st.error(str(exc))

        if st.button("Reset dataset"):
            st.session_state.working[name] = source_df
            st.session_state.configs[name] = {}
            st.rerun()


with tab_merge:
    st.subheader("Merge & compare datasets")
    names = list(st.session_state.working.keys())

    if len(names) < 2:
        st.info("Load at least two datasets to merge or compare them.")
    else:
        st.markdown("#### Join two sources")
        j1, j2, j3 = st.columns([1, 1, 0.7])
        left_name = j1.selectbox("Left dataset", names, key="merge_left")
        right_options = [n for n in names if n != left_name]
        right_name = j2.selectbox("Right dataset", right_options, key="merge_right")
        join_mode = j3.selectbox("Join type", ["left", "inner", "full"], key="merge_mode")

        left_df = st.session_state.working[left_name]
        right_df = st.session_state.working[right_name]
        common_columns = [c for c in left_df.columns if c in right_df.columns]
        join_keys = st.multiselect("Join key(s)", common_columns, key="merge_keys")

        if st.button("Create merged dataset", type="primary", disabled=not join_keys):
            try:
                merged = merge_frames(left_df, right_df, join_keys, join_mode)
                merged_name = f"{left_name}__merged__{right_name}"
                register_frames({merged_name: merged})
                st.success(f"Created {merged_name} with {merged.height:,} row(s).")
            except Exception as exc:
                st.error(str(exc))

        if database_available():
            with st.expander("Save / reuse merge recipe"):
                recipe_label = st.text_input(
                    "Merge recipe name",
                    placeholder="Provider directory + pricing merge",
                    key="merge_recipe_name",
                )
                if st.button(
                    "Save current merge recipe",
                    disabled=not (recipe_label and join_keys),
                    key="save_merge_recipe_button",
                ):
                    try:
                        save_merge_recipe(
                            recipe_label,
                            left_name,
                            right_name,
                            join_keys,
                            join_mode,
                        )
                        st.success(f"Saved merge recipe: {recipe_label}")
                    except Exception as exc:
                        st.error(str(exc))

                try:
                    merge_recipes = list_merge_recipes()
                except Exception as exc:
                    merge_recipes = []
                    st.warning(f"Could not load merge recipes: {exc}")

                if merge_recipes:
                    recipe_map = {row["name"]: row for row in merge_recipes}
                    selected_recipe_name = st.selectbox(
                        "Saved merge recipe",
                        list(recipe_map.keys()),
                        key="saved_merge_recipe",
                    )
                    selected_recipe = recipe_map[selected_recipe_name]
                    st.caption(
                        f"{selected_recipe['left_dataset']} + "
                        f"{selected_recipe['right_dataset']} on "
                        f"{', '.join(selected_recipe['join_keys'])} "
                        f"({selected_recipe['join_mode']})"
                    )

                    can_apply = (
                        selected_recipe["left_dataset"] in st.session_state.working
                        and selected_recipe["right_dataset"] in st.session_state.working
                    )
                    if st.button(
                        "Run saved merge recipe",
                        disabled=not can_apply,
                        key="run_saved_merge_recipe",
                    ):
                        try:
                            recipe_left = st.session_state.working[selected_recipe["left_dataset"]]
                            recipe_right = st.session_state.working[selected_recipe["right_dataset"]]
                            merged = merge_frames(
                                recipe_left,
                                recipe_right,
                                list(selected_recipe["join_keys"]),
                                selected_recipe["join_mode"],
                            )
                            merged_name = (
                                f"{selected_recipe['left_dataset']}__merged__"
                                f"{selected_recipe['right_dataset']}"
                            )
                            register_frames({merged_name: merged})
                            st.success(
                                f"Ran '{selected_recipe_name}'. "
                                f"Created {merged_name} with {merged.height:,} row(s)."
                            )
                        except Exception as exc:
                            st.error(str(exc))
                    elif not can_apply:
                        st.caption(
                            "Load both datasets named in the saved recipe before running it."
                        )

        st.markdown("#### Compare two versions")
        d1, d2 = st.columns(2)
        previous_name = d1.selectbox("Previous version", names, key="diff_previous")
        current_options = [n for n in names if n != previous_name]
        current_name = d2.selectbox("Current version", current_options, key="diff_current")

        previous_df = st.session_state.working[previous_name]
        current_df = st.session_state.working[current_name]
        diff_common = [c for c in previous_df.columns if c in current_df.columns]
        diff_keys = st.multiselect("Comparison key(s)", diff_common, key="diff_keys")

        if st.button("Build change report", disabled=not diff_keys):
            try:
                diff = diff_frames(previous_df, current_df, diff_keys)
                diff_name = f"{previous_name}__vs__{current_name}"
                register_frames({diff_name: diff})
                counts = (
                    diff.group_by("__change_status")
                    .len()
                    .sort("__change_status")
                    .to_dicts()
                )
                st.success(f"Created {diff_name}.")
                st.dataframe(counts, use_container_width=True, hide_index=True)
                st.dataframe(diff.head(200).to_pandas(), use_container_width=True, hide_index=True)
            except Exception as exc:
                st.error(str(exc))


with tab_match:
    st.subheader("Match & geocode")
    name = selected_dataset("match_dataset")

    if not name:
        st.info("Import a dataset first.")
    else:
        df = st.session_state.working[name]

        st.markdown("#### Build a normalized address key")
        a1, a2, a3 = st.columns(3)
        street_col = a1.selectbox("Street", [""] + df.columns, key="addr_street")
        city_col = a2.selectbox("City", [""] + df.columns, key="addr_city")
        state_col = a3.selectbox("State / province", [""] + df.columns, key="addr_state")
        a4, a5 = st.columns(2)
        postal_col = a4.selectbox("Postal code (optional)", [""] + df.columns, key="addr_postal")
        country_col = a5.selectbox("Country (optional)", [""] + df.columns, key="addr_country")

        if st.button(
            "Add normalized address key",
            disabled=not (street_col and city_col and state_col),
        ):
            try:
                updated = add_address_key(
                    df,
                    street_col,
                    city_col,
                    state_col,
                    postal=postal_col or None,
                    country=country_col or None,
                )
                st.session_state.working[name] = updated
                df = updated
                st.success("Added __address_key.")
                st.dataframe(
                    updated.select(
                        [c for c in [street_col, city_col, state_col, postal_col, country_col, "__address_key"] if c]
                    ).head(100).to_pandas(),
                    use_container_width=True,
                    hide_index=True,
                )
            except Exception as exc:
                st.error(str(exc))

        st.markdown("#### Find likely duplicate entities")
        f1, f2 = st.columns([1, 1])
        fuzzy_name = f1.selectbox("Name / entity column", [""] + df.columns, key="fuzzy_name")
        fuzzy_blocks = f2.multiselect(
            "Block within columns (recommended: city/state)",
            df.columns,
            key="fuzzy_blocks",
        )
        f3, f4 = st.columns(2)
        threshold = f3.slider("Similarity threshold", 50, 100, 92, key="fuzzy_threshold")
        max_pairs = f4.number_input(
            "Maximum candidate comparisons",
            min_value=100,
            max_value=500000,
            value=50000,
            step=1000,
            key="fuzzy_max_pairs",
        )

        if st.button("Find fuzzy duplicate pairs", disabled=not fuzzy_name):
            try:
                pairs = find_fuzzy_duplicates(
                    df,
                    fuzzy_name,
                    block_columns=fuzzy_blocks,
                    threshold=float(threshold),
                    max_pairs=int(max_pairs),
                )
                pair_name = f"{name}__fuzzy_pairs"
                register_frames({pair_name: pairs})
                if pairs.height:
                    st.warning(f"Found {pairs.height:,} candidate duplicate pair(s).")
                    st.dataframe(pairs.head(500).to_pandas(), use_container_width=True, hide_index=True)
                else:
                    st.success("No candidate duplicate pairs met the selected threshold.")
            except Exception as exc:
                st.error(str(exc))

        st.markdown("#### U.S. Census geocoding")
        address_options = [""] + st.session_state.working[name].columns
        address_col = st.selectbox(
            "Full address column",
            address_options,
            index=address_options.index("__address_key") if "__address_key" in address_options else 0,
            key="geocode_address",
        )
        g1, g2 = st.columns(2)
        geocode_limit = g1.number_input(
            "Maximum unique addresses this run",
            min_value=1,
            max_value=10000,
            value=500,
            step=100,
        )
        workers = g2.slider("Concurrent requests", 1, 8, 4)

        if st.button("Geocode selected addresses", disabled=not address_col):
            try:
                with st.spinner("Geocoding addresses..."):
                    geocoded = geocode_dataframe(
                        st.session_state.working[name],
                        address_col,
                        max_rows=int(geocode_limit),
                        workers=int(workers),
                    )
                st.session_state.working[name] = geocoded
                counts = (
                    geocoded.group_by("__geocode_status")
                    .len()
                    .sort("__geocode_status")
                    .to_dicts()
                )
                st.success("Geocoding run completed.")
                st.dataframe(counts, use_container_width=True, hide_index=True)
                preview_cols = [
                    c for c in [
                        address_col,
                        "__geocode_status",
                        "__matched_address",
                        "latitude",
                        "longitude",
                    ] if c in geocoded.columns
                ]
                st.dataframe(
                    geocoded.select(preview_cols).head(200).to_pandas(),
                    use_container_width=True,
                    hide_index=True,
                )
            except Exception as exc:
                st.error(str(exc))


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

        quarantine_name = f"{name}__quarantine"
        quarantine_df = st.session_state.working.get(quarantine_name)
        package = to_validation_zip(
            df,
            name,
            profile=profile_frame(df),
            quarantine=quarantine_df,
            transform_config=st.session_state.configs.get(name) or None,
        )
        st.download_button(
            "Download validation package (.zip)",
            data=package,
            file_name=f"{name}_validation_package.zip",
            mime="application/zip",
            use_container_width=True,
            help="Includes clean CSV, quarantine CSV when available, column profile, transform recipe, and manifest.",
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


with tab_sources:
    st.subheader("Sources & refresh")
    if not database_available():
        st.info("Set DATABASE_URL to enable saved source URLs and automatic refresh tracking.")
    else:
        st.markdown("#### Register a reusable source")
        try:
            recipe_rows = list_recipes()
            recipe_names = [""] + [row["name"] for row in recipe_rows]
        except Exception:
            recipe_names = [""]

        with st.form("source_registry_form"):
            s1, s2 = st.columns(2)
            source_name = s1.text_input("Source name", placeholder="CDC Adult Vaccination Coverage")
            source_url = s2.text_input("Direct source URL", placeholder="https://.../dataset.csv")
            s3, s4, s5, s6 = st.columns([1, 1, 0.45, 0.7])
            target_table = s3.text_input("Target table", placeholder="cdc_adult_coverage")
            recipe_name = s4.selectbox("Transform recipe", recipe_names)
            active = s5.checkbox("Active", value=True)
            archive_raw = s6.checkbox(
                "Archive raw",
                value=False,
                help="Store each changed source file in Uploadcare when UPLOADCARE_PUBLIC_KEY is configured.",
            )
            cadence_map = {
                "Hourly": 60,
                "Every 6 hours": 360,
                "Every 12 hours": 720,
                "Daily": 1440,
                "Weekly": 10080,
            }
            cadence_label = st.selectbox(
                "Automatic refresh cadence",
                list(cadence_map.keys()),
                index=3,
            )
            refresh_interval_minutes = cadence_map[cadence_label]
            saved = st.form_submit_button("Save source", type="primary")

        if saved:
            if not source_name or not source_url or not target_table:
                st.error("Source name, URL, and target table are required.")
            else:
                try:
                    save_source(
                        source_name,
                        source_url,
                        target_table,
                        recipe_name=recipe_name or None,
                        active=active,
                        archive_raw=archive_raw,
                        refresh_interval_minutes=refresh_interval_minutes,
                    )
                    st.success(f"Saved source: {source_name}")
                except Exception as exc:
                    st.error(str(exc))

        try:
            sources = list_sources()
        except Exception as exc:
            sources = []
            st.error(f"Could not load sources: {exc}")

        if sources:
            st.markdown("#### Registered sources")
            display_rows = []
            for row in sources:
                display_rows.append({
                    "id": row["id"],
                    "name": row["name"],
                    "target_table": row["target_table"],
                    "recipe": row.get("recipe_name"),
                    "active": row["active"],
                    "archive_raw": row.get("archive_raw"),
                    "cadence_minutes": row.get("refresh_interval_minutes"),
                    "status": row.get("last_status"),
                    "last_checked": row.get("last_checked_at"),
                    "last_changed": row.get("last_changed_at"),
                    "last_error": row.get("last_error"),
                    "archive_url": row.get("last_archive_url"),
                    "url": row["url"],
                })
            st.dataframe(display_rows, use_container_width=True, hide_index=True)

            labels = {
                f"#{row['id']} — {row['name']}": row["id"]
                for row in sources
            }
            selected_label = st.selectbox("Source to refresh", list(labels.keys()))
            force_refresh = st.checkbox(
                "Force refresh even if source bytes are unchanged",
                value=False,
            )
            r1, r2 = st.columns(2)

            with r1:
                if st.button("Refresh selected source", type="primary"):
                    try:
                        result = refresh_source(labels[selected_label], force=force_refresh)
                        if result["status"] == "unchanged":
                            st.info("Source checked successfully. No content change detected.")
                        else:
                            st.success(
                                f"Source refreshed: {result['rows']:,} row(s) written across "
                                f"{len(result['tables'])} table(s)."
                            )
                            if result["tables"]:
                                st.dataframe(result["tables"], use_container_width=True, hide_index=True)
                            if result.get("archive"):
                                st.success(f"Raw source archived: {result['archive']['url']}")
                            if result.get("archive_warning"):
                                st.warning(result["archive_warning"])
                    except Exception as exc:
                        st.error(str(exc))

            with r2:
                if st.button("Refresh all active sources"):
                    try:
                        results = refresh_all_active_sources(force=force_refresh)
                        st.dataframe(results, use_container_width=True, hide_index=True)
                        failures = [row for row in results if row.get("status") == "error"]
                        if failures:
                            st.warning(f"{len(failures)} source(s) failed. See the results table.")
                        else:
                            st.success(f"Checked {len(results)} active source(s).")
                    except Exception as exc:
                        st.error(str(exc))

            st.caption(
                "The source refresher hashes each downloaded file. Unchanged files are skipped; "
                "changed files are transformed with the selected recipe and replace the target table "
                "with the existing table snapshotted first."
            )
        else:
            st.caption("No reusable sources registered yet.")


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
