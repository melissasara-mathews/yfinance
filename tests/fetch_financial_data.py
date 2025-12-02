# get_whsmith_financials.py
# Pulls WH Smith (SMWH.L) financial statements (annual + quarterly), filters 2022–2025, saves tidy CSVs.

import argparse
from pathlib import Path
import pandas as pd
import yfinance as yf

def tidy_statement(df: pd.DataFrame, statement_name: str, period: str, ticker: str):
    """
    yfinance returns statements with metrics as rows, columns = dates (Datetimes or Timestamps).
    We transpose, keep numeric, and melt to long tidy form.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker","statement","period","date","metric","value"])
    # Ensure columns are datetime
    cols = []
    for c in df.columns:
        try:
            cols.append(pd.to_datetime(c))
        except Exception:
            cols.append(pd.NaT)
    df.columns = cols
    df = df.loc[:, ~df.columns.isna()]
    # transpose -> rows=dates, columns=metrics
    t = df.T
    t.index.name = "date"
    t.reset_index(inplace=True)
    long = t.melt(id_vars=["date"], var_name="metric", value_name="value")
    long["ticker"] = ticker
    long["statement"] = statement_name
    long["period"] = period
    # drop obvious empties
    long = long.dropna(subset=["date"]).sort_values(["date","metric"])
    return long[["ticker","statement","period","date","metric","value"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="SMWH.L", help="Yahoo Finance ticker (default: SMWH.L)")
    ap.add_argument("--outdir", default="data/whsmith", help="Output directory")
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--end",   default="2025-12-31")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    tk = yf.Ticker(args.ticker)

    # Raw statements (wide)
    inc_a = tk.financials                # annual income
    inc_q = tk.quarterly_financials      # quarterly income
    bs_a  = tk.balance_sheet             # annual balance
    bs_q  = tk.quarterly_balance_sheet   # quarterly balance
    cf_a  = tk.cashflow                  # annual cash flow
    cf_q  = tk.quarterly_cashflow        # quarterly cash flow

    # Tidy long
    inc_a_t = tidy_statement(inc_a, "income_statement", "annual", args.ticker)
    inc_q_t = tidy_statement(inc_q, "income_statement", "quarterly", args.ticker)
    bs_a_t  = tidy_statement(bs_a,  "balance_sheet",    "annual", args.ticker)
    bs_q_t  = tidy_statement(bs_q,  "balance_sheet",    "quarterly", args.ticker)
    cf_a_t  = tidy_statement(cf_a,  "cashflow",         "annual", args.ticker)
    cf_q_t  = tidy_statement(cf_q,  "cashflow",         "quarterly", args.ticker)

    # Combine & filter to 2022–2025
    all_tidy = pd.concat([inc_a_t, inc_q_t, bs_a_t, bs_q_t, cf_a_t, cf_q_t], ignore_index=True)
    all_tidy["date"] = pd.to_datetime(all_tidy["date"])
    mask = (all_tidy["date"] >= pd.Timestamp(args.start)) & (all_tidy["date"] <= pd.Timestamp(args.end))
    all_tidy_2225 = all_tidy.loc[mask].copy()

    # Save tidy
    all_tidy.to_csv(outdir / "smwh_financials_all_tidy_full.csv", index=False)
    all_tidy_2225.to_csv(outdir / "smwh_financials_all_tidy_2022_2025.csv", index=False)

    # Also save per-statement filtered files for convenience
    def save_subset(df, stmt, period, fname):
        sub = df[(df["statement"]==stmt) & (df["period"]==period)]
        sub.to_csv(outdir / fname, index=False)
        return sub.shape

    shapes = {
        "inc_q":  save_subset(all_tidy_2225, "income_statement","quarterly","smwh_income_quarterly_2022_2025_tidy.csv"),
        "inc_a":  save_subset(all_tidy_2225, "income_statement","annual",   "smwh_income_annual_2022_2025_tidy.csv"),
        "bs_q":   save_subset(all_tidy_2225, "balance_sheet","quarterly",   "smwh_balance_quarterly_2022_2025_tidy.csv"),
        "bs_a":   save_subset(all_tidy_2225, "balance_sheet","annual",      "smwh_balance_annual_2022_2025_tidy.csv"),
        "cf_q":   save_subset(all_tidy_2225, "cashflow","quarterly",        "smwh_cashflow_quarterly_2022_2025_tidy.csv"),
        "cf_a":   save_subset(all_tidy_2225, "cashflow","annual",           "smwh_cashflow_annual_2022_2025_tidy.csv"),
    }

    # Raw (wide) exports too (if present)
    if inc_q is not None and not inc_q.empty: inc_q.to_csv(outdir/"smwh_raw_income_quarterly.csv")
    if inc_a is not None and not inc_a.empty: inc_a.to_csv(outdir/"smwh_raw_income_annual.csv")
    if bs_q  is not None and not bs_q.empty:  bs_q.to_csv(outdir/"smwh_raw_balance_quarterly.csv")
    if bs_a  is not None and not bs_a.empty:  bs_a.to_csv(outdir/"smwh_raw_balance_annual.csv")
    if cf_q  is not None and not cf_q.empty:  cf_q.to_csv(outdir/"smwh_raw_cashflow_quarterly.csv")
    if cf_a  is not None and not cf_a.empty:  cf_a.to_csv(outdir/"smwh_raw_cashflow_annual.csv")

    print("\n[SUMMARY 2022–2025 rows, cols] ->", shapes)
    print("\nFiles written to:", outdir.resolve())

if __name__ == "__main__":
    main()
