# app.py (stable stateful version)
import io
from decimal import Decimal, InvalidOperation
from typing import Optional
import pandas as pd
import streamlit as st

# -----------------------
# 基础设置
# -----------------------
st.set_page_config(page_title="收款对账助手 | Treasury Matcher", layout="wide")
st.title("收款对账助手 Treasury Matcher")

# -----------------------
# 工具函数
# -----------------------
def _to_decimal_series(s: pd.Series) -> pd.Series:
    def to_dec(x):
        if pd.isna(x):
            return pd.NA
        try:
            return Decimal(str(x).replace(",", "").strip())
        except (InvalidOperation, AttributeError):
            return pd.NA
    return s.map(to_dec)

def match_treasury_query(
    df_query: pd.DataFrame,
    df_treasury: pd.DataFrame,
    name_col_q: str = "客户姓名",
    amount_col_q: str = "实付金额",
    date_col_q: str = "下单时间",
    name_col_t: str = "客户姓名",
    amount_col_t: str = "收款金额",
    date_col_t: Optional[str] = "收款日期",
    amount_tol: float = 0.01,
    suffix_query: str = "query",
    suffix_treasury: str = "treasury",
) -> pd.DataFrame:

    q = df_query.copy()
    t = df_treasury.copy()

    q["_idx_q"] = q.index
    t["_idx_t"] = t.index
    q[name_col_q] = q[name_col_q].astype(str).str.strip()
    t[name_col_t] = t[name_col_t].astype(str).str.strip()
    q["_amt_q"] = _to_decimal_series(q[amount_col_q])
    t["_amt_t"] = _to_decimal_series(t[amount_col_t])

    if date_col_q in q.columns:
        q["_ordertime"] = pd.to_datetime(q[date_col_q], errors="coerce")
    else:
        q["_ordertime"] = pd.NaT

    cand = q.merge(
        t,
        left_on=name_col_q,
        right_on=name_col_t,
        how="inner",
        suffixes=(f"_{suffix_query}", f"_{suffix_treasury}")
    )
    tol_dec = Decimal(str(amount_tol))
    cand["_amt_diff"] = (cand["_amt_q"] - cand["_amt_t"]).abs()
    cand_valid = cand[cand["_amt_diff"] <= tol_dec].copy()

    cand_valid.sort_values(by=["_amt_diff", "_ordertime"], inplace=True)
    cand_q_first = cand_valid.drop_duplicates(subset=["_idx_q"], keep="first")
    matched = cand_q_first.drop_duplicates(subset=["_idx_t"], keep="first")

    matched_q_idx = set(matched["_idx_q"].tolist())
    matched_t_idx = set(matched["_idx_t"].tolist())

    q_unmatched = q[~q["_idx_q"].isin(matched_q_idx)].copy()
    t_unmatched = t[~t["_idx_t"].isin(matched_t_idx)].copy()

    q_cols_orig = df_query.columns.tolist()
    t_cols_orig = df_treasury.columns.tolist()

    matched["_source"] = "matched"
    matched["_amount_match_within_tol"] = True
    out_matched = matched.copy()

    for col in t_cols_orig:
        if col not in q_unmatched.columns:
            q_unmatched[col] = pd.NA
    for col in q_cols_orig:
        if col not in t_unmatched.columns:
            t_unmatched[col] = pd.NA

    def add_prefix_for_side(df, cols, prefix):
        rename_map = {c: f"{prefix}{c}" for c in cols if c in df.columns}
        return df.rename(columns=rename_map)

    out_matched = add_prefix_for_side(out_matched, q_cols_orig, f"{suffix_query}.")
    out_matched = add_prefix_for_side(out_matched, t_cols_orig, f"{suffix_treasury}.")

    q_unmatched_pref = add_prefix_for_side(q_unmatched[q_cols_orig], q_cols_orig, f"{suffix_query}.")
    for c in t_cols_orig:
        q_unmatched_pref[f"{suffix_treasury}.{c}"] = pd.NA
    q_unmatched_pref["_source"] = "query_only"
    q_unmatched_pref[f"{suffix_query}._idx_q"] = q_unmatched["_idx_q"]
    q_unmatched_pref[f"{suffix_treasury}._idx_t"] = pd.NA
    q_unmatched_pref["_amt_diff"] = pd.NA
    q_unmatched_pref["_amount_match_within_tol"] = False
    q_unmatched_pref["_ordertime"] = q["_ordertime"][q_unmatched["_idx_q"]].values

    t_unmatched_pref = add_prefix_for_side(t_unmatched[t_cols_orig], t_cols_orig, f"{suffix_treasury}.")
    for c in q_cols_orig:
        t_unmatched_pref[f"{suffix_query}.{c}"] = pd.NA
    t_unmatched_pref["_source"] = "treasury_only"
    t_unmatched_pref[f"{suffix_query}._idx_q"] = pd.NA
    t_unmatched_pref[f"{suffix_treasury}._idx_t"] = t_unmatched["_idx_t"]
    t_unmatched_pref["_amt_diff"] = pd.NA
    t_unmatched_pref["_amount_match_within_tol"] = False
    t_unmatched_pref["_ordertime"] = pd.NaT

    if "_ordertime" not in out_matched.columns and f"{suffix_query}.{date_col_q}" in out_matched.columns:
        out_matched["_ordertime"] = pd.to_datetime(
            out_matched[f"{suffix_query}.{date_col_q}"], errors="coerce"
        )
    elif "_ordertime" not in out_matched.columns:
        out_matched["_ordertime"] = pd.NaT

    out = pd.concat([out_matched, q_unmatched_pref, t_unmatched_pref], ignore_index=True)
    out.sort_values(by="_ordertime", inplace=True, kind="mergesort")
    out.reset_index(drop=True, inplace=True)

    q_name = f"{suffix_query}.{name_col_q}"
    q_amt  = f"{suffix_query}.{amount_col_q}"
    q_date = f"{suffix_query}.{date_col_q}"
    t_name = f"{suffix_treasury}.{name_col_t}"
    t_amt  = f"{suffix_treasury}.{amount_col_t}"
    t_date_full = f"{suffix_treasury}.{date_col_t}" if (date_col_t and f"{suffix_treasury}.{date_col_t}" in out.columns) else None

    final_cols = {
        "客户姓名_query": out[q_name] if q_name in out.columns else pd.NA,
        "实付金额": out[q_amt] if q_amt in out.columns else pd.NA,
        "下单时间": out[q_date] if q_date in out.columns else pd.NaT,
        "客户姓名_treasury": out[t_name] if t_name in out.columns else pd.NA,
        "收款金额": out[t_amt] if t_amt in out.columns else pd.NA,
        "匹配结果": out["_source"].eq("matched")
    }
    if t_date_full:
        final_cols["收款日期"] = out[t_date_full]
    else:
        final_cols["收款日期"] = pd.NaT

    final = pd.DataFrame(final_cols).sort_values(by="下单时间", kind="mergesort").reset_index(drop=True)

    mask_true_missing_name = final["匹配结果"] & final["客户姓名_treasury"].isna()
    final.loc[mask_true_missing_name, "客户姓名_treasury"] = final.loc[mask_true_missing_name, "客户姓名_query"]

    return final

# -----------------------
# 缓存：把“解析Excel字节 -> DataFrame”缓存住
# 传入 bytes，这样每次重跑不用重新上传
# -----------------------
@st.cache_data(show_spinner=False)
def read_excel_sheets(file_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    return xls.sheet_names

@st.cache_data(show_spinner=False)
def read_excel_df(file_bytes: bytes, sheet_name: str):
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)

# -----------------------
# 侧边栏参数
# -----------------------
with st.sidebar:
    st.header("参数设置 / Settings")
    amount_tol = st.number_input("金额容差（元）Amount Tolerance", min_value=0.0, value=0.01, step=0.01)
    st.caption("说明：默认0.01（分级别）。如因手续费/四舍五入导致差异，可适当放大。")

# -----------------------
# 上传区（单文件 或 两文件）
# -----------------------
tab1, tab2 = st.tabs(["📄 单文件（含两个Sheet）", "🗂️ 分别上传两个文件"])

# 统一的 session 键名
SS = st.session_state

# ====== Tab1：单文件，先选择sheet，再读取 ======
with tab1:
    file_one = st.file_uploader("上传单个Excel（同时包含 Query & Treasury 两个工作表）", type=["xlsx", "xls"], key="file_one_uploader")
    if file_one is not None:
        # 把文件内容持久化为 bytes，避免组件重跑后丢失
        SS["file_one_bytes"] = file_one.getvalue()
        # 解析 sheet 列表（缓存）
        sheet_names = read_excel_sheets(SS["file_one_bytes"])

        with st.form("form_read_single", clear_on_submit=False):
            c1, c2 = st.columns(2)
            with c1:
                sheet_q = st.selectbox("选择业务明细Sheet（Query）", options=sheet_names, key="sheet_q")
            with c2:
                sheet_t = st.selectbox("选择财务流水Sheet（Treasury）", options=sheet_names, index=min(1, len(sheet_names)-1), key="sheet_t")

            submitted = st.form_submit_button("读取该Excel", type="primary")
            if submitted:
                SS["df_query"] = read_excel_df(SS["file_one_bytes"], SS["sheet_q"])
                SS["df_treasury"] = read_excel_df(SS["file_one_bytes"], SS["sheet_t"])
                st.success("读取完成 / Loaded.")
                st.rerun()

# ====== Tab2：分别上传两文件，直接读取到会话 ======
with tab2:
    file_q = st.file_uploader("上传业务明细（Query）Excel", type=["xlsx", "xls"], key="file_q_uploader")
    file_t = st.file_uploader("上传财务流水（Treasury）Excel", type=["xlsx", "xls"], key="file_t_uploader")

    if file_q is not None:
        SS["file_q_bytes"] = file_q.getvalue()
        # 默认读取第一个sheet
        SS["df_query"] = pd.read_excel(io.BytesIO(SS["file_q_bytes"]), sheet_name=0)
    if file_t is not None:
        SS["file_t_bytes"] = file_t.getvalue()
        SS["df_treasury"] = pd.read_excel(io.BytesIO(SS["file_t_bytes"]), sheet_name=0)
    if (file_q is not None) and (file_t is not None):
        st.success("两个文件读取完成 / Both loaded.")

# -----------------------
# 映射与运行（使用表单，避免半途交互导致状态丢失）
# -----------------------
if ("df_query" in SS) and ("df_treasury" in SS):
    df_query = SS["df_query"]
    df_treasury = SS["df_treasury"]

    st.subheader("列名映射 / Column Mapping")
    with st.form("form_mapping", clear_on_submit=False):
        c1, c2 = st.columns(2)

        # 用固定顺序的 list(df.columns) 作为 options，避免乱序
        q_cols = list(df_query.columns)
        t_cols = list(df_treasury.columns)

        with c1:
            st.markdown("**业务明细（Query）列选择**")
            q_name = st.selectbox("客户姓名（Query）", options=q_cols, key="q_name")
            q_amt  = st.selectbox("实付金额（Query）", options=q_cols, key="q_amt")
            q_date = st.selectbox("下单时间（Query）", options=q_cols, key="q_date")

        with c2:
            st.markdown("**财务流水（Treasury）列选择**")
            t_name = st.selectbox("客户姓名（Treasury）", options=t_cols, key="t_name")
            t_amt  = st.selectbox("收款金额（Treasury）", options=t_cols, key="t_amt")
            t_date_opts = ["<无/None>"] + t_cols
            t_date_sel = st.selectbox("收款日期（可选）", options=t_date_opts, key="t_date_sel")

        run = st.form_submit_button("开始匹配 / Run Matching", type="primary")

    if run:
        t_date = None if SS.get("t_date_sel") in (None, "<无/None>") else SS["t_date_sel"]
        try:
            result = match_treasury_query(
                df_query=df_query,
                df_treasury=df_treasury,
                name_col_q=SS["q_name"],
                amount_col_q=SS["q_amt"],
                date_col_q=SS["q_date"],
                name_col_t=SS["t_name"],
                amount_col_t=SS["t_amt"],
                date_col_t=t_date,
                amount_tol=amount_tol
            )
            st.success(f"匹配完成：共 {len(result)} 行。")
            st.dataframe(result, use_container_width=True, hide_index=True)

            csv_bytes = result.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ 下载CSV", data=csv_bytes, file_name="treasury_matching_result.csv", mime="text/csv")

            xlsx_buffer = io.BytesIO()
            with pd.ExcelWriter(xlsx_buffer, engine="xlsxwriter") as writer:
                result.to_excel(writer, index=False, sheet_name="result")
            st.download_button("⬇️ 下载Excel（XLSX）", data=xlsx_buffer.getvalue(),
                               file_name="treasury_matching_result.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"匹配失败：{e}")
else:
    st.info("请先上传数据文件 / Please upload your files.")
