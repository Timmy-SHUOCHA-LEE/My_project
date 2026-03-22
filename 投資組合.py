# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import webbrowser
import time
import tempfile
import shutil
import re
from datetime import datetime, timedelta

import requests
import pandas as pd
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta


# --- 第一部分：儀表板內容 ---
def run_streamlit_app():
    import streamlit as st
    from FinMind.data import DataLoader
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    st.set_page_config(
        page_title="5475 / 3234 / 3105 / 3037 / 0050 / TAIEX 走勢對比 + 投資組合分析 + 法人籌碼 + 券商分點 + 大戶籌碼 + 融資融券借券",
        layout="wide"
    )
    st.title("📈 5475、3234、3105、3037、0050 與加權指數 (TAIEX) 走勢對比分析")
    st.caption("含：走勢對比 / 累計報酬 / 相對大盤超額績效 / 個人投資組合分析 / 法人籌碼差異 / 券商分點前三名 / 大戶籌碼 / 融資融券借券")

    # =============================
    # 基本參數
    # =============================
    FINMIND_TOKEN = os.getenv(
        "FINMIND_TOKEN",
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMy0xOCAyMjozNToxNyIsInVzZXJfaWQiOiI4OTEwMDciLCJlbWFpbCI6ImxpbGxhcmQ4MDZAZ21haWwuY29tIiwiaXAiOiIxMTQuMzYuMTgxLjcxIn0.WEgK_gYl-WQfRxxMR9bVrvkMsltT1pHfO_TEmvvIsMU"
    )

    WANTGOO_USERNAME = os.getenv("WANTGOO_USERNAME", "lillard1006@gmail.com")
    WANTGOO_PASSWORD = os.getenv("WANTGOO_PASSWORD", "lillard@80613")
    WANTGOO_HEADLESS = os.getenv("WANTGOO_HEADLESS", "false").lower() == "true"

    FINMIND_BASE_URL = "https://api.finmindtrade.com/api/v4/data"

    TARGET_STOCK_IDS = ["0050", "3234", "5475", "3105", "3037"]

    stock_name_map_local = {
        "0050": "0050",
        "3234": "光環 (3234)",
        "5475": "德宏 (5475)",
        "3105": "穩懋 (3105)",
        "3037": "欣興 (3037)"
    }

    # =============================
    # FinMind 載入
    # =============================
    @st.cache_resource
    def get_loader():
        api = DataLoader()
        try:
            api.login_by_token(api_token=FINMIND_TOKEN)
        except AttributeError:
            api.login(token=FINMIND_TOKEN)
        return api

    @st.cache_data(ttl=3600)
    def load_market_data(start_date_main, start_date_0050, end_date):
        try:
            api = get_loader()

            stock_config = {
                "5475": start_date_main,
                "3234": start_date_main,
                "3105": start_date_main,
                "3037": start_date_main,
                "0050": start_date_0050,
                "TAIEX": start_date_main,
            }

            raw_data = {}
            for sid, sdate in stock_config.items():
                df = api.taiwan_stock_daily(
                    stock_id=sid,
                    start_date=sdate,
                    end_date=end_date
                )
                raw_data[sid] = df

            if any(df is None or df.empty for df in raw_data.values()):
                return None, "部分標的無法取得資料"

            def process_df(df):
                df = df.rename(columns={
                    "date": "Date",
                    "open": "Open",
                    "max": "High",
                    "min": "Low",
                    "close": "Close",
                    "Trading_Volume": "Volume",
                    "volume": "Volume"
                })

                df["Date"] = pd.to_datetime(df["Date"])
                df = df.sort_values("Date")
                df.set_index("Date", inplace=True)

                for col in ["Open", "High", "Low", "Close", "Volume"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                return df

            processed = {sid: process_df(df) for sid, df in raw_data.items()}

            common_index = None
            for sid, df in processed.items():
                if common_index is None:
                    common_index = df.index
                else:
                    common_index = common_index.intersection(df.index)

            aligned = {sid: df.loc[common_index].copy() for sid, df in processed.items()}
            return aligned, None

        except Exception as e:
            return None, str(e)

    # =============================
    # 0050 定期定額模擬
    # =============================
    @st.cache_data(ttl=3600)
    def simulate_0050_dca(df_0050, sim_start_date_str):
        if df_0050 is None or df_0050.empty:
            return pd.DataFrame(), 0.0, 0.0

        df = df_0050.copy().sort_index()
        df["季均價"] = df["Close"].rolling(window=60, min_periods=20).mean()

        trading_dates = df.index.sort_values()

        sim_start_date = pd.to_datetime(sim_start_date_str).normalize()
        last_available_date = trading_dates.max().normalize()

        if sim_start_date > last_available_date:
            return pd.DataFrame(), 0.0, 0.0

        month_cursor = pd.Timestamp(sim_start_date.year, sim_start_date.month, 1)
        month_end = pd.Timestamp(last_available_date.year, last_available_date.month, 1)

        scheduled_days = [1, 7, 13, 19, 25]
        records = []
        used_trade_dates = set()

        while month_cursor <= month_end:
            year = month_cursor.year
            month = month_cursor.month

            for day in scheduled_days:
                try:
                    planned_date = pd.Timestamp(year=year, month=month, day=day)
                except ValueError:
                    continue

                if planned_date < sim_start_date or planned_date > last_available_date:
                    continue

                candidate_dates = trading_dates[trading_dates >= planned_date]
                if len(candidate_dates) == 0:
                    continue

                trade_date = candidate_dates[0]
                if trade_date in used_trade_dates:
                    continue

                loc = df.index.get_loc(trade_date)
                if loc == 0:
                    continue

                prev_trade_date = df.index[loc - 1]
                prev_close = float(df.loc[prev_trade_date, "Close"])
                quarter_avg = df.loc[prev_trade_date, "季均價"]

                if pd.isna(quarter_avg) or quarter_avg == 0:
                    invest_amount = 1000
                    diff_pct = None
                else:
                    diff_pct = (prev_close / quarter_avg - 1) * 100
                    if diff_pct <= -5:
                        invest_amount = 2000
                    elif diff_pct >= 5:
                        invest_amount = 800
                    else:
                        invest_amount = 1000

                buy_price = float(df.loc[trade_date, "Close"])
                buy_shares = invest_amount / buy_price if buy_price != 0 else 0

                records.append({
                    "原定扣款日": planned_date,
                    "實際扣款日": trade_date,
                    "前一交易日": prev_trade_date,
                    "前日收盤價": prev_close,
                    "前日季均價(60日)": float(quarter_avg) if pd.notna(quarter_avg) else None,
                    "偏離季均價(%)": diff_pct,
                    "扣款金額": invest_amount,
                    "買入價格": buy_price,
                    "新增股數": buy_shares,
                })

                used_trade_dates.add(trade_date)

            month_cursor = month_cursor + relativedelta(months=1)

        dca_df = pd.DataFrame(records)
        if dca_df.empty:
            return dca_df, 0.0, 0.0

        total_dca_cost = float(dca_df["扣款金額"].sum())
        total_dca_shares = float(dca_df["新增股數"].sum())
        return dca_df, total_dca_cost, total_dca_shares

    # =============================
    # 投資組合
    # =============================
    @st.cache_data(ttl=3600)
    def build_portfolio_df(df_5475, df_3234, df_0050, dca_df=None):
        if dca_df is None:
            dca_df = pd.DataFrame()

        base_portfolio_data = [
            {
                "標的": "德宏 (5475)",
                "代號": "5475",
                "成本價": 108.5,
                "原始總成本": 108543,
                "最新收盤價": float(df_5475["Close"].iloc[-1]),
            },
            {
                "標的": "光環 (3234)",
                "代號": "3234",
                "成本價": 86.47,
                "原始總成本": 129751,
                "最新收盤價": float(df_3234["Close"].iloc[-1]),
            },
            {
                "標的": "0050",
                "代號": "0050",
                "成本價": 53.92,
                "原始總成本": 48054,
                "最新收盤價": float(df_0050["Close"].iloc[-1]),
            },
        ]

        df_portfolio = pd.DataFrame(base_portfolio_data)
        df_portfolio["原始持有股數(推估)"] = df_portfolio["原始總成本"] / df_portfolio["成本價"]

        dca_cost_0050 = 0.0
        dca_shares_0050 = 0.0
        if not dca_df.empty:
            dca_cost_0050 = float(dca_df["扣款金額"].sum())
            dca_shares_0050 = float(dca_df["新增股數"].sum())

        df_portfolio["定期定額新增成本"] = 0.0
        df_portfolio["定期定額新增股數"] = 0.0

        mask_0050 = df_portfolio["代號"] == "0050"
        df_portfolio.loc[mask_0050, "定期定額新增成本"] = dca_cost_0050
        df_portfolio.loc[mask_0050, "定期定額新增股數"] = dca_shares_0050

        df_portfolio["總成本"] = df_portfolio["原始總成本"] + df_portfolio["定期定額新增成本"]
        df_portfolio["持有股數(推估)"] = df_portfolio["原始持有股數(推估)"] + df_portfolio["定期定額新增股數"]

        df_portfolio["最新市值"] = df_portfolio["持有股數(推估)"] * df_portfolio["最新收盤價"]
        df_portfolio["損益"] = df_portfolio["最新市值"] - df_portfolio["總成本"]
        df_portfolio["報酬率(%)"] = (df_portfolio["損益"] / df_portfolio["總成本"]) * 100

        total_cost = float(df_portfolio["總成本"].sum())
        total_value = float(df_portfolio["最新市值"].sum())
        total_profit = float(df_portfolio["損益"].sum())
        total_return = (total_profit / total_cost) * 100 if total_cost != 0 else 0.0

        return df_portfolio, total_cost, total_value, total_profit, total_return

    # =============================
    # 法人資料
    # =============================
    @st.cache_data(ttl=3600)
    def load_institutional_data(stock_id, start_date, end_date, is_market_total=False):
        api = get_loader()

        if is_market_total:
            df = api.taiwan_stock_institutional_investors_total(
                start_date=start_date,
                end_date=end_date
            )
        else:
            df = api.taiwan_stock_institutional_investors(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
        df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
        df["net"] = df["buy"] - df["sell"]

        return df.sort_values("date")

    def summarize_institutional_table(raw_df, target_name, last_n_days=10):
        if raw_df is None or raw_df.empty:
            return pd.DataFrame()

        df = raw_df.copy()
        grouped = df.groupby(["date", "name"], as_index=False)["net"].sum()
        pivot_df = grouped.pivot(index="date", columns="name", values="net").fillna(0)

        def get_series(col_name):
            if col_name in pivot_df.columns:
                return pivot_df[col_name]
            return pd.Series(0, index=pivot_df.index, dtype="float64")

        foreign = get_series("Foreign_Investor") + get_series("Foreign_Dealer_Self")
        investment_trust = get_series("Investment_Trust")
        dealer_total = get_series("Dealer_self") + get_series("Dealer_Hedging")
        total_3 = foreign + investment_trust + dealer_total

        result = pd.DataFrame({
            "日期": pivot_df.index,
            "外資買賣超": foreign.values,
            "投信買賣超": investment_trust.values,
            "自營商買賣超": dealer_total.values,
            "三大法人買賣超": total_3.values
        }).sort_values("日期")

        result = result.tail(last_n_days).copy()
        result["標的"] = target_name

        cols = ["標的", "日期", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人買賣超"]
        return result[cols]

    def build_recommendation(one_target_df, target_name):
        if one_target_df is None or one_target_df.empty:
            return {
                "標的": target_name,
                "近三天外資買賣超合計": None,
                "近三天投信買賣超合計": None,
                "近三天自營商買賣超合計": None,
                "近三天三大法人買賣超合計": None,
                "法人建議": "無資料",
                "法人說明": "查無法人資料"
            }

        recent3 = one_target_df.sort_values("日期").tail(3)

        foreign_3d = float(recent3["外資買賣超"].sum())
        trust_3d = float(recent3["投信買賣超"].sum())
        dealer_3d = float(recent3["自營商買賣超"].sum())
        total3_3d = float(recent3["三大法人買賣超"].sum())

        if trust_3d > 0:
            suggestion = "不買"
            reason = "投信近三天買超，依你的規則優先判定為不買"
        elif (foreign_3d < 0) and (total3_3d < 0):
            suggestion = "不買"
            reason = "外資與三大法人近三天皆為賣超"
        elif (foreign_3d > 0) or (total3_3d > 0):
            suggestion = "買"
            reason = "外資或三大法人近三天至少一項買超"
        else:
            suggestion = "觀望"
            reason = "近三天未符合明確買進或不買條件"

        return {
            "標的": target_name,
            "近三天外資買賣超合計": foreign_3d,
            "近三天投信買賣超合計": trust_3d,
            "近三天自營商買賣超合計": dealer_3d,
            "近三天三大法人買賣超合計": total3_3d,
            "法人建議": suggestion,
            "法人說明": reason
        }

    def style_net_table(df):
        def color_net(val):
            try:
                val = float(val)
                if val > 0:
                    return "color: red; font-weight: bold;"
                elif val < 0:
                    return "color: green; font-weight: bold;"
                return ""
            except Exception:
                return ""

        numeric_cols = [
            "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人買賣超",
            "近三天外資買賣超合計", "近三天投信買賣超合計",
            "近三天自營商買賣超合計", "近三天三大法人買賣超合計"
        ]
        existing_numeric_cols = [c for c in numeric_cols if c in df.columns]

        styler = df.style
        if existing_numeric_cols:
            styler = styler.format({col: "{:,.0f}" for col in existing_numeric_cols})
            styler = styler.map(color_net, subset=existing_numeric_cols)

        return styler

    # =============================
    # 融資 / 融券 / 借券
    # =============================
    def fetch_finmind_data(dataset, data_id, start_date, end_date, token):
        headers = {
            "Authorization": f"Bearer {token}"
        }
        params = {
            "dataset": dataset,
            "data_id": data_id,
            "start_date": start_date,
            "end_date": end_date,
        }

        r = requests.get(FINMIND_BASE_URL, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        if "data" not in data:
            raise ValueError(f"{dataset} 回傳格式異常：{data}")

        return pd.DataFrame(data["data"])

    @st.cache_data(ttl=3600)
    def load_margin_short_lending_data(stock_id, start_date, end_date, token):
        try:
            df_margin = fetch_finmind_data(
                dataset="TaiwanStockMarginPurchaseShortSale",
                data_id=stock_id,
                start_date=start_date,
                end_date=end_date,
                token=token
            )

            if df_margin.empty:
                return pd.DataFrame(), f"{stock_id} 融資融券資料為空"

            df_margin["date"] = pd.to_datetime(df_margin["date"])

            margin_cols = [
                "date",
                "stock_id",
                "MarginPurchaseTodayBalance",
                "ShortSaleTodayBalance"
            ]

            for col in margin_cols:
                if col not in df_margin.columns:
                    raise KeyError(f"{stock_id} 融資融券資料缺少欄位：{col}")

            df_margin = df_margin[margin_cols].copy()

            df_lending = fetch_finmind_data(
                dataset="TaiwanStockSecuritiesLending",
                data_id=stock_id,
                start_date=start_date,
                end_date=end_date,
                token=token
            )

            if df_lending.empty:
                df_lending_daily = pd.DataFrame(columns=["date", "SecuritiesLendingVolume"])
            else:
                df_lending["date"] = pd.to_datetime(df_lending["date"])

                if "volume" not in df_lending.columns:
                    raise KeyError(f"{stock_id} 借券資料缺少欄位：volume")

                df_lending_daily = (
                    df_lending.groupby("date", as_index=False)["volume"]
                    .sum()
                    .rename(columns={"volume": "SecuritiesLendingVolume"})
                )

            df = pd.merge(
                df_margin,
                df_lending_daily,
                on="date",
                how="left"
            )

            df["SecuritiesLendingVolume"] = df["SecuritiesLendingVolume"].fillna(0)
            df = df.sort_values("date").tail(7).reset_index(drop=True)
            df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

            df["Prev_Margin"] = df["MarginPurchaseTodayBalance"].shift(1)
            df["Prev_Short"] = df["ShortSaleTodayBalance"].shift(1)
            df["Prev_Lending"] = df["SecuritiesLendingVolume"].shift(1)

            def get_signal(row):
                if pd.isna(row["Prev_Margin"]) or pd.isna(row["Prev_Short"]) or pd.isna(row["Prev_Lending"]):
                    return "無前日資料"

                if (
                    row["MarginPurchaseTodayBalance"] > row["Prev_Margin"]
                    and row["ShortSaleTodayBalance"] < row["Prev_Short"]
                ):
                    return "建議不買"

                elif row["SecuritiesLendingVolume"] < row["Prev_Lending"]:
                    return "建議買"

                else:
                    return "觀望"

            df["Signal"] = df.apply(get_signal, axis=1)

            plot_df = df.copy()

            if len(plot_df) > 0 and plot_df["MarginPurchaseTodayBalance"].iloc[0] != 0:
                plot_df["Margin_Index"] = (
                    plot_df["MarginPurchaseTodayBalance"] / plot_df["MarginPurchaseTodayBalance"].iloc[0] * 100
                )
            else:
                plot_df["Margin_Index"] = 100

            if len(plot_df) > 0 and plot_df["ShortSaleTodayBalance"].iloc[0] != 0:
                plot_df["Short_Index"] = (
                    plot_df["ShortSaleTodayBalance"] / plot_df["ShortSaleTodayBalance"].iloc[0] * 100
                )
            else:
                plot_df["Short_Index"] = 100

            if len(plot_df) > 0 and plot_df["SecuritiesLendingVolume"].iloc[0] != 0:
                plot_df["Lending_Index"] = (
                    plot_df["SecuritiesLendingVolume"] / plot_df["SecuritiesLendingVolume"].iloc[0] * 100
                )
            else:
                base = plot_df["SecuritiesLendingVolume"].replace(0, pd.NA).dropna()
                if len(base) > 0:
                    first_valid = base.iloc[0]
                    plot_df["Lending_Index"] = plot_df["SecuritiesLendingVolume"] / first_valid * 100
                else:
                    plot_df["Lending_Index"] = 100

            return plot_df, None

        except Exception as e:
            return pd.DataFrame(), str(e)

    @st.cache_data(ttl=3600)
    def load_all_margin_short_lending(stock_ids, start_date, end_date, token):
        result = {}
        for sid in stock_ids:
            df, err = load_margin_short_lending_data(sid, start_date, end_date, token)
            result[sid] = {
                "df": df,
                "error": err
            }
        return result

    def build_margin_signal_summary(margin_data_map):
        rows = []
        for sid in TARGET_STOCK_IDS:
            item = margin_data_map.get(sid, {})
            df = item.get("df", pd.DataFrame())
            err = item.get("error")

            if err:
                rows.append({
                    "標的": stock_name_map_local.get(sid, sid),
                    "最新日期": "",
                    "最新融資餘額": None,
                    "最新融券餘額": None,
                    "最新借券量": None,
                    "融資融券借券建議": "無資料",
                    "融資融券借券說明": err
                })
                continue

            if df is None or df.empty:
                rows.append({
                    "標的": stock_name_map_local.get(sid, sid),
                    "最新日期": "",
                    "最新融資餘額": None,
                    "最新融券餘額": None,
                    "最新借券量": None,
                    "融資融券借券建議": "無資料",
                    "融資融券借券說明": "查無資料"
                })
                continue

            latest = df.iloc[-1]
            signal = latest.get("Signal", "無資料")

            if signal == "建議買":
                final_signal = "買"
            elif signal == "建議不買":
                final_signal = "不買"
            elif signal == "觀望":
                final_signal = "觀望"
            else:
                final_signal = "無資料"

            rows.append({
                "標的": stock_name_map_local.get(sid, sid),
                "最新日期": latest.get("date_str", ""),
                "最新融資餘額": latest.get("MarginPurchaseTodayBalance"),
                "最新融券餘額": latest.get("ShortSaleTodayBalance"),
                "最新借券量": latest.get("SecuritiesLendingVolume"),
                "融資融券借券建議": final_signal,
                "融資融券借券說明": signal
            })

        return pd.DataFrame(rows)

    def prepare_margin_detail_table(df):
        if df is None or df.empty:
            return pd.DataFrame(columns=[
                "date_str",
                "MarginPurchaseTodayBalance",
                "ShortSaleTodayBalance",
                "SecuritiesLendingVolume",
                "Signal"
            ])

        show_df = df.copy()
        show_df = show_df[[
            "date_str",
            "MarginPurchaseTodayBalance",
            "ShortSaleTodayBalance",
            "SecuritiesLendingVolume",
            "Signal"
        ]].rename(columns={
            "date_str": "日期",
            "MarginPurchaseTodayBalance": "融資餘額",
            "ShortSaleTodayBalance": "融券餘額",
            "SecuritiesLendingVolume": "借券量",
            "Signal": "建議"
        })
        return show_df

    def plot_margin_short_lending_chart(plot_df, stock_label):
        if plot_df is None or plot_df.empty:
            return None

        fig, ax1 = plt.subplots(figsize=(14, 5))

        ax1.plot(
            plot_df["date_str"],
            plot_df["Margin_Index"],
            marker="o",
            linewidth=2,
            label="融資餘額指數化"
        )

        ax1.plot(
            plot_df["date_str"],
            plot_df["Short_Index"],
            marker="o",
            linewidth=2,
            label="融券餘額指數化"
        )

        ax2 = ax1.twinx()
        ax2.plot(
            plot_df["date_str"],
            plot_df["Lending_Index"],
            marker="o",
            linewidth=2.2,
            label="借券量指數化"
        )

        ax1.set_xlabel("Date")
        ax1.set_ylabel("")
        ax1.tick_params(axis="y", left=False, labelleft=False)
        ax1.set_yticks([])
        ax1.spines["left"].set_visible(False)
        ax1.spines["top"].set_visible(False)

        ax2.set_ylabel("")
        ax2.tick_params(axis="y", right=False, labelright=False)
        ax2.set_yticks([])
        ax2.spines["right"].set_visible(False)
        ax2.spines["top"].set_visible(False)

        try:
            ymin = min(
                plot_df["Margin_Index"].min(),
                plot_df["Short_Index"].min(),
                plot_df["Lending_Index"].min()
            )
            ymax = max(
                plot_df["Margin_Index"].max(),
                plot_df["Short_Index"].max(),
                plot_df["Lending_Index"].max()
            )
            padding = max((ymax - ymin) * 0.08, 3)
            ax1.set_ylim(ymin - padding, ymax + padding)
            ax2.set_ylim(ax1.get_ylim())
        except Exception:
            pass

        lines_1, labels_1 = ax1.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left", ncol=3)

        plt.title(f"{stock_label} 近一週融資 / 融券 / 借券走勢", fontsize=14)
        fig.tight_layout()

        return fig

    # =============================
    # WantGoo / Selenium 區
    # =============================
    def safe_click(driver, element):
        try:
            element.click()
        except Exception:
            driver.execute_script("arguments[0].click();", element)

    def _get_cell_text(driver, cell, retries=5, pause=0.25):
        for _ in range(retries):
            try:
                text = cell.get_attribute("innerText")
                if text and text.strip():
                    return text.strip()

                text = cell.get_attribute("textContent")
                if text and text.strip():
                    return text.strip()

                text = driver.execute_script(
                    "return arguments[0].innerText || arguments[0].textContent || '';",
                    cell
                )
                if text and str(text).strip():
                    return str(text).strip()
            except Exception:
                pass

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cell)
            except Exception:
                pass

            time.sleep(pause)

        return ""

    def _parse_float_from_text(text):
        if text is None:
            return None
        text = str(text).strip()
        if not text:
            return None

        text = text.replace("%", "").replace(",", "").strip()
        m = re.search(r"-?\d+(?:\.\d+)?", text)
        if not m:
            return None
        try:
            return float(m.group())
        except Exception:
            return None

    def build_chrome_driver(headless=False):
        profile_dir = tempfile.mkdtemp(prefix="wantgoo_chrome_")

        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        if headless:
            options.add_argument("--headless=new")

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(40)

        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                    """
                }
            )
        except Exception:
            pass

        return driver, profile_dir

    def maybe_login_wantgoo(driver, wait, username, password):
        driver.get("https://www.wantgoo.com/")
        time.sleep(3)

        login_selectors = [
            (By.CSS_SELECTOR, "#unregistered-bar a.topbar-nav__a"),
            (By.XPATH, "//a[contains(., '登入')]"),
            (By.XPATH, "//button[contains(., '登入')]"),
        ]

        login_btn = None
        for by, sel in login_selectors:
            try:
                elems = driver.find_elements(by, sel)
                if elems:
                    login_btn = elems[0]
                    break
            except Exception:
                pass

        if login_btn is None:
            return

        try:
            safe_click(driver, login_btn)
            time.sleep(2)
        except Exception:
            return

        user_candidates = [
            (By.CSS_SELECTOR, 'input[c-model="userName"]'),
            (By.CSS_SELECTOR, 'input[type="email"]'),
            (By.XPATH, "//input[contains(@placeholder, 'Email')]"),
            (By.XPATH, "//input[contains(@placeholder, '帳號')]"),
        ]
        pass_candidates = [
            (By.CSS_SELECTOR, 'input[c-model="password"]'),
            (By.CSS_SELECTOR, 'input[type="password"]'),
        ]
        submit_candidates = [
            (By.CSS_SELECTOR, 'button[login=""]'),
            (By.XPATH, "//button[contains(., '登入')]"),
            (By.XPATH, "//button[contains(., 'Sign in')]"),
        ]

        user_input = None
        pass_input = None
        submit_btn = None

        for by, sel in user_candidates:
            try:
                user_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((by, sel))
                )
                if user_input:
                    break
            except Exception:
                pass

        for by, sel in pass_candidates:
            try:
                pass_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((by, sel))
                )
                if pass_input:
                    break
            except Exception:
                pass

        for by, sel in submit_candidates:
            try:
                submit_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((by, sel))
                )
                if submit_btn:
                    break
            except Exception:
                pass

        if user_input and pass_input and submit_btn:
            user_input.clear()
            user_input.send_keys(username)
            pass_input.clear()
            pass_input.send_keys(password)
            safe_click(driver, submit_btn)
            time.sleep(4)

    # -----------------------------
    # 券商分點
    # -----------------------------
    def open_branch_buysell_page(driver, stock_no):
        target_urls = [
            f"https://www.wantgoo.com/stock/{stock_no}/major-investors/branch-buysell",
            f"https://www.wantgoo.com/stock/{stock_no}/major-investors",
            f"https://www.wantgoo.com/stock/{stock_no}",
        ]

        last_err = None
        for url in target_urls:
            try:
                driver.get(url)
                time.sleep(3)
                if stock_no in driver.current_url:
                    return
            except Exception as e:
                last_err = e

        if last_err:
            raise last_err

    def switch_to_1_day(driver):
        try:
            date_selector = WebDriverWait(driver, 6).until(
                EC.presence_of_element_located((By.ID, "dateSelector"))
            )
            driver.execute_script("""
                arguments[0].value = "1";
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            """, date_selector)
            time.sleep(3)
            return
        except Exception:
            pass

        text_candidates = ["近1日", "近 1 日", "1日", "近一日"]
        for txt in text_candidates:
            xpath_list = [
                f"//*[normalize-space(text())='{txt}']",
                f"//button[contains(normalize-space(.), '{txt}')]",
                f"//a[contains(normalize-space(.), '{txt}')]",
                f"//li[contains(normalize-space(.), '{txt}')]",
                f"//span[contains(normalize-space(.), '{txt}')]",
                f"//option[contains(normalize-space(.), '{txt}')]",
            ]
            for xp in xpath_list:
                try:
                    elem = WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located((By.XPATH, xp))
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
                    time.sleep(0.5)
                    safe_click(driver, elem)
                    time.sleep(3)
                    return
                except Exception:
                    pass

    def get_table_rows(driver):
        candidate_selectors = [
            "tbody.rt tr",
            "table tbody tr",
            ".rt-tbody .rt-tr-group",
            ".rt-table .rt-tr",
        ]

        last_err = None
        for selector in candidate_selectors:
            try:
                rows = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                rows = [r for r in rows if r.is_displayed()]
                if len(rows) >= 3:
                    return rows
            except Exception as e:
                last_err = e

        if last_err:
            raise last_err
        raise RuntimeError("找不到券商分點表格列資料")

    def extract_broker_name(driver, row):
        td_candidates = [
            "td:nth-child(2)",
            "div[role='cell']:nth-child(2)",
            ".rt-td:nth-child(2)"
        ]

        for selector in td_candidates:
            try:
                cell = row.find_element(By.CSS_SELECTOR, selector)
                name = _get_cell_text(driver, cell, retries=6, pause=0.2)
                if name:
                    return name.strip()
            except Exception:
                pass

        try:
            tds = row.find_elements(By.TAG_NAME, "td")
            if len(tds) >= 2:
                return _get_cell_text(driver, tds[1], retries=6, pause=0.2)
        except Exception:
            pass

        return ""

    def analyze_broker_branch(driver, stock_no):
        open_branch_buysell_page(driver, stock_no)
        switch_to_1_day(driver)
        rows = get_table_rows(driver)

        if len(rows) < 3:
            return {
                "stock_id": stock_no,
                "buy_top3": [],
                "sell_top3": [],
                "broker_signal": "無資料",
                "broker_reason": "表格資料不足"
            }

        top3_rows = rows[:3]
        bottom3_rows = rows[-3:]

        buy_top3 = [extract_broker_name(driver, r) for r in top3_rows]
        sell_top3 = [extract_broker_name(driver, r) for r in reversed(bottom3_rows)]

        buy_top3 = [x for x in buy_top3 if x]
        sell_top3 = [x for x in sell_top3 if x]

        buy_bad_list = [
            "元大土城學",
            "凱基台北",
            "凱基信義",
            "富邦台北",
            "美商高盛亞",
            "凱基松山"
        ]

        sell_bad_list = [
            "摩根大通",
            "摩根士丹利"
        ]

        buy_good_list = [
            "摩根大通",
            "摩根士丹利"
        ]

        buy_bad_count = 0
        buy_good_found = False
        buy_good_matches = []
        sell_bad_found = False
        sell_bad_matches = []

        for b in buy_top3:
            for bad in buy_bad_list:
                if bad.lower() in b.lower():
                    buy_bad_count += 1
                    break

        for s in sell_top3:
            for bad in sell_bad_list:
                if bad.lower() in s.lower():
                    sell_bad_found = True
                    sell_bad_matches.append(s)
                    break

        for b in buy_top3:
            for good in buy_good_list:
                if good.lower() in b.lower():
                    buy_good_found = True
                    buy_good_matches.append(b)
                    break

        if buy_good_found and not (buy_bad_count >= 2 or sell_bad_found):
            broker_signal = "買"
            broker_reason = f"買超前三名中出現 {', '.join(buy_good_matches)}"
        elif (buy_bad_count >= 2 or sell_bad_found) and not buy_good_found:
            reasons = []
            if buy_bad_count >= 2:
                reasons.append("買超前三名中包含兩個或以上的指定偏空券商")
            if sell_bad_found:
                reasons.append(f"賣超前三名中出現 {', '.join(sell_bad_matches)}")
            broker_signal = "不買"
            broker_reason = "；".join(reasons)
        elif buy_good_found and (buy_bad_count >= 2 or sell_bad_found):
            reasons = [f"偏多：買超前三名中出現 {', '.join(buy_good_matches)}"]
            if buy_bad_count >= 2:
                reasons.append("偏空：買超前三名中包含兩個或以上的指定偏空券商")
            if sell_bad_found:
                reasons.append(f"偏空：賣超前三名中出現 {', '.join(sell_bad_matches)}")
            broker_signal = "觀望"
            broker_reason = "；".join(reasons)
        else:
            broker_signal = "觀望"
            broker_reason = "無明確建議買或不建議買訊號，建議觀察"

        return {
            "stock_id": stock_no,
            "buy_top3": buy_top3,
            "sell_top3": sell_top3,
            "broker_signal": broker_signal,
            "broker_reason": broker_reason
        }

    # -----------------------------
    # 大戶籌碼
    # -----------------------------
    def go_to_stock_page(driver, wait, stock_no):
        target_urls = [
            f"https://www.wantgoo.com/stock/{stock_no}",
            f"https://www.wantgoo.com/stock/{stock_no}/major-investors",
        ]

        for url in target_urls:
            try:
                driver.get(url)
                time.sleep(3)
                if stock_no in driver.current_url:
                    return
            except Exception:
                pass

        search_selectors = [
            (By.CSS_SELECTOR, "input.frm-control.frm-control--sm.typeahead.tt-input"),
            (By.CSS_SELECTOR, "input[type='search']"),
            (By.XPATH, "//input[contains(@placeholder,'搜尋')]"),
        ]
        for by, sel in search_selectors:
            try:
                search_input = wait.until(EC.presence_of_element_located((by, sel)))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", search_input)
                time.sleep(1)

                search_input.click()
                search_input.send_keys(Keys.CONTROL, "a")
                search_input.send_keys(Keys.DELETE)
                search_input.send_keys(stock_no)
                time.sleep(1)
                search_input.send_keys(Keys.ENTER)
                time.sleep(5)

                if stock_no in driver.current_url:
                    return
            except Exception:
                pass

        driver.get(f"https://www.wantgoo.com/stock/{stock_no}")
        time.sleep(3)

    def open_major_holders_tab(driver, wait):
        candidates = [
            (By.XPATH, "//a[contains(normalize-space(.), '大戶籌碼')]"),
            (By.XPATH, "//button[contains(normalize-space(.), '大戶籌碼')]"),
            (By.XPATH, "//*[contains(normalize-space(.), '大戶籌碼')]"),
        ]

        for by, sel in candidates:
            try:
                elem = wait.until(EC.element_to_be_clickable((by, sel)))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
                time.sleep(1)
                safe_click(driver, elem)
                time.sleep(3)
                return True
            except Exception:
                pass
        return False

    def get_major_holder_rows(driver, wait):
        selectors = [
            (By.CSS_SELECTOR, "tr[concentration-item]"),
            (By.CSS_SELECTOR, "tbody tr[concentration-item]"),
            (By.CSS_SELECTOR, "table tbody tr"),
        ]

        for by, sel in selectors:
            try:
                rows = wait.until(EC.presence_of_all_elements_located((by, sel)))
                rows = [r for r in rows if r.is_displayed()]
                if len(rows) >= 2:
                    return rows
            except Exception:
                pass
        return []

    def extract_major_holder_info_from_row(row):
        date_text = ""
        rate_text = ""

        try:
            date_text = row.find_element(By.CSS_SELECTOR, "td[c-model='date']").text.strip()
        except Exception:
            pass

        try:
            rate_text = row.find_element(By.CSS_SELECTOR, "td[c-model='rateOfDistribution']").text.strip()
        except Exception:
            pass

        if not date_text or not rate_text:
            try:
                tds = row.find_elements(By.TAG_NAME, "td")
                td_texts = [td.text.strip() for td in tds if td.text and td.text.strip()]

                if not date_text and len(td_texts) >= 1:
                    date_text = td_texts[0]

                if not rate_text:
                    for t in td_texts:
                        if "%" in t or re.search(r"\d+(?:\.\d+)?", t):
                            parsed = _parse_float_from_text(t)
                            if parsed is not None:
                                rate_text = t
                                break
            except Exception:
                pass

        rate_value = _parse_float_from_text(rate_text)
        return date_text, rate_value

    def analyze_major_investors(driver, wait, stock_no):
        try:
            go_to_stock_page(driver, wait, stock_no)

            opened = open_major_holders_tab(driver, wait)
            if not opened:
                return {
                    "stock_id": stock_no,
                    "this_week_date": None,
                    "this_week_rate": None,
                    "last_week_date": None,
                    "last_week_rate": None,
                    "major_signal": "無資料",
                    "major_reason": "找不到大戶籌碼頁籤"
                }

            rows = get_major_holder_rows(driver, wait)
            if len(rows) < 2:
                return {
                    "stock_id": stock_no,
                    "this_week_date": None,
                    "this_week_rate": None,
                    "last_week_date": None,
                    "last_week_rate": None,
                    "major_signal": "無資料",
                    "major_reason": "找不到足夠的大戶籌碼歷史資料"
                }

            this_week_date, this_week_rate = extract_major_holder_info_from_row(rows[0])
            last_week_date, last_week_rate = extract_major_holder_info_from_row(rows[1])

            if this_week_rate is None or last_week_rate is None:
                return {
                    "stock_id": stock_no,
                    "this_week_date": this_week_date,
                    "this_week_rate": this_week_rate,
                    "last_week_date": last_week_date,
                    "last_week_rate": last_week_rate,
                    "major_signal": "無資料",
                    "major_reason": "大戶持股比例解析失敗"
                }

            if this_week_rate < last_week_rate:
                major_signal = "不買"
                major_reason = f"最新一週大戶比例 {this_week_rate:.2f}% 低於前一週 {last_week_rate:.2f}%，大戶籌碼減少"
            elif this_week_rate > last_week_rate:
                major_signal = "買"
                major_reason = f"最新一週大戶比例 {this_week_rate:.2f}% 高於前一週 {last_week_rate:.2f}%，大戶籌碼增加"
            else:
                major_signal = "觀望"
                major_reason = f"最新一週大戶比例 {this_week_rate:.2f}% 與前一週 {last_week_rate:.2f}% 持平"

            return {
                "stock_id": stock_no,
                "this_week_date": this_week_date,
                "this_week_rate": this_week_rate,
                "last_week_date": last_week_date,
                "last_week_rate": last_week_rate,
                "major_signal": major_signal,
                "major_reason": major_reason
            }

        except Exception as e:
            return {
                "stock_id": stock_no,
                "this_week_date": None,
                "this_week_rate": None,
                "last_week_date": None,
                "last_week_rate": None,
                "major_signal": "無資料",
                "major_reason": f"{stock_no} 大戶籌碼抓取失敗：{e}"
            }

    def load_wantgoo_all_signals(username, password, stock_ids, headless=False):
        if not username or not password:
            return {
                sid: {
                    "stock_id": sid,
                    "buy_top3": [],
                    "sell_top3": [],
                    "broker_signal": "無資料",
                    "broker_reason": "未提供 WantGoo 帳號密碼",
                    "this_week_date": None,
                    "this_week_rate": None,
                    "last_week_date": None,
                    "last_week_rate": None,
                    "major_signal": "無資料",
                    "major_reason": "未提供 WantGoo 帳號密碼"
                }
                for sid in stock_ids
            }

        driver = None
        profile_dir = None

        try:
            driver, profile_dir = build_chrome_driver(headless=headless)
            wait = WebDriverWait(driver, 20)

            maybe_login_wantgoo(driver, wait, username, password)

            results = {}
            for sid in stock_ids:
                stock_result = {"stock_id": sid}

                try:
                    broker_result = analyze_broker_branch(driver, sid)
                    stock_result.update(broker_result)
                except Exception as e:
                    stock_result.update({
                        "buy_top3": [],
                        "sell_top3": [],
                        "broker_signal": "無資料",
                        "broker_reason": f"{sid} 券商分點抓取失敗：{e}"
                    })

                try:
                    major_result = analyze_major_investors(driver, wait, sid)
                    stock_result.update(major_result)
                except Exception as e:
                    stock_result.update({
                        "this_week_date": None,
                        "this_week_rate": None,
                        "last_week_date": None,
                        "last_week_rate": None,
                        "major_signal": "無資料",
                        "major_reason": f"{sid} 大戶籌碼抓取失敗：{e}"
                    })

                results[sid] = stock_result

            return results

        except Exception as e:
            return {
                sid: {
                    "stock_id": sid,
                    "buy_top3": [],
                    "sell_top3": [],
                    "broker_signal": "無資料",
                    "broker_reason": f"券商分點抓取失敗：{e}",
                    "this_week_date": None,
                    "this_week_rate": None,
                    "last_week_date": None,
                    "last_week_rate": None,
                    "major_signal": "無資料",
                    "major_reason": f"大戶籌碼抓取失敗：{e}"
                }
                for sid in stock_ids
            }
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

            if profile_dir:
                try:
                    shutil.rmtree(profile_dir, ignore_errors=True)
                except Exception:
                    pass

    def combine_suggestion(*signals):
        normalized = []
        for x in signals:
            if x is None:
                continue
            x = str(x).strip()
            if x in ["買", "不買", "觀望"]:
                normalized.append(x)

        if not normalized:
            return "無資料"

        if "不買" in normalized:
            return "不買"
        if "買" in normalized:
            return "買"
        return "觀望"

    def build_broker_top3_table(wantgoo_results):
        rows = []

        for sid in TARGET_STOCK_IDS:
            data = wantgoo_results.get(sid, {})
            buy_top3 = data.get("buy_top3", [])
            sell_top3 = data.get("sell_top3", [])

            row = {
                "標的": stock_name_map_local.get(sid, sid),
                "買超第1名": buy_top3[0] if len(buy_top3) > 0 else "",
                "買超第2名": buy_top3[1] if len(buy_top3) > 1 else "",
                "買超第3名": buy_top3[2] if len(buy_top3) > 2 else "",
                "賣超第1名": sell_top3[0] if len(sell_top3) > 0 else "",
                "賣超第2名": sell_top3[1] if len(sell_top3) > 1 else "",
                "賣超第3名": sell_top3[2] if len(sell_top3) > 2 else "",
                "券商分點建議": data.get("broker_signal", "無資料"),
                "券商分點說明": data.get("broker_reason", "")
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def build_major_holder_table(wantgoo_results):
        rows = []
        for sid in TARGET_STOCK_IDS:
            data = wantgoo_results.get(sid, {})
            rows.append({
                "標的": stock_name_map_local.get(sid, sid),
                "最新一週日期": data.get("this_week_date") or "",
                "最新一週大戶比例(%)": data.get("this_week_rate"),
                "前一週日期": data.get("last_week_date") or "",
                "前一週大戶比例(%)": data.get("last_week_rate"),
                "大戶籌碼建議": data.get("major_signal", "無資料"),
                "大戶籌碼說明": data.get("major_reason", "")
            })
        return pd.DataFrame(rows)

    # =============================
    # 活的日期參數
    # =============================
    today = datetime.today()
    one_year_ago = today - relativedelta(years=1)

    start_date_main = one_year_ago.strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    start_date_0050 = "2025-06-18"

    margin_start_date = (today.date() - timedelta(days=20)).strftime("%Y-%m-%d")
    margin_end_date = today.date().strftime("%Y-%m-%d")

    data_map, err = load_market_data(
        start_date_main=start_date_main,
        start_date_0050=start_date_0050,
        end_date=end_date
    )

    if err:
        st.error(f"⚠️ 數據讀取錯誤：{err}")
        return

    if not data_map:
        st.error("❌ 無法取得數據，請檢查 API Token、額度、股票代碼或起始日期。")
        return

    df_5475 = data_map["5475"]
    df_3234 = data_map["3234"]
    df_3105 = data_map["3105"]
    df_3037 = data_map["3037"]
    df_0050 = data_map["0050"]
    df_index = data_map["TAIEX"]

    st.caption(
        f"5475 / 3234 / 3105 / 3037 / TAIEX 區間：{start_date_main} ~ {end_date}；"
        f"0050 區間：{start_date_0050} ~ {end_date}（非營業日自動跳過）"
    )

    color_5475 = "#d65f4a"
    color_3234 = "#9467bd"
    color_3105 = "#ff7f0e"
    color_3037 = "#17becf"
    color_0050 = "#2ca02c"
    color_index = "#5b9bd5"

    stock_name_map = {
        "5475": "德宏 (5475)",
        "3234": "光環 (3234)",
        "3105": "穩懋 (3105)",
        "3037": "欣興 (3037)",
        "0050": "0050",
        "TAIEX": "加權指數 (TAIEX)"
    }

    stock_color_map = {
        "5475": color_5475,
        "3234": color_3234,
        "3105": color_3105,
        "3037": color_3037,
        "0050": color_0050,
        "TAIEX": color_index
    }

    # =============================
    # 0050 定期定額模擬
    # =============================
    sim_start_date = start_date_0050
    dca_df, dca_total_cost, dca_total_shares = simulate_0050_dca(df_0050, sim_start_date)

    # =============================
    # 投資組合總覽
    # =============================
    st.subheader("💼 我的投資組合總覽")

    df_portfolio, total_cost, total_value, total_profit, total_return = build_portfolio_df(
        df_5475, df_3234, df_0050, dca_df=dca_df
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("總投入成本", f"${total_cost:,.0f}")
    c2.metric("目前總市值", f"${total_value:,.0f}")
    c3.metric("總損益", f"${total_profit:,.0f}")
    c4.metric("總報酬率", f"{total_return:.2f}%")

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("#### 🥧 個股持有比例（以總成本計算）")
        pie_labels = df_portfolio["標的"]
        pie_values = df_portfolio["總成本"]
        pie_colors = [color_5475, color_3234, color_0050]

        fig_pie, ax_pie = plt.subplots(figsize=(7, 7), facecolor="none")
        ax_pie.set_facecolor("none")

        wedges, texts, autotexts = ax_pie.pie(
            pie_values,
            labels=pie_labels,
            autopct="%1.1f%%",
            startangle=90,
            colors=pie_colors,
            textprops={"fontsize": 11, "color": "white"}
        )

        ax_pie.set_title("投資組合持有比例", color="white")
        ax_pie.axis("equal")

        for text in texts:
            text.set_color("white")
        for autotext in autotexts:
            autotext.set_color("white")

        st.pyplot(fig_pie, transparent=True)

    with col_right:
        st.markdown("#### 📋 投資組合明細")
        display_df = df_portfolio.copy()

        def color_profit(val):
            try:
                val = float(val)
                if val > 0:
                    return "color: red; font-weight: bold;"
                elif val < 0:
                    return "color: green; font-weight: bold;"
                return ""
            except Exception:
                return ""

        st.dataframe(
            display_df.style.format({
                "成本價": "{:.2f}",
                "原始總成本": "{:,.0f}",
                "定期定額新增成本": "{:,.0f}",
                "總成本": "{:,.0f}",
                "最新收盤價": "{:.2f}",
                "原始持有股數(推估)": "{:.3f}",
                "定期定額新增股數": "{:.3f}",
                "持有股數(推估)": "{:.3f}",
                "最新市值": "{:,.0f}",
                "損益": "{:,.0f}",
                "報酬率(%)": "{:.2f}",
            }).map(color_profit, subset=["損益", "報酬率(%)"]),
            use_container_width=True
        )

    st.markdown("#### 🧾 投資組合摘要")
    best_profit_row = df_portfolio.loc[df_portfolio["報酬率(%)"].idxmax()]
    worst_profit_row = df_portfolio.loc[df_portfolio["報酬率(%)"].idxmin()]

    st.markdown(
        f"""
- 目前總投入：**${total_cost:,.0f}**
- 目前總市值：**${total_value:,.0f}**
- 目前總損益：**${total_profit:,.0f}**
- 目前總報酬率：**{total_return:.2f}%**
- 0050 定期定額累計新增成本：**${dca_total_cost:,.0f}**
- 0050 定期定額累計新增股數：**{dca_total_shares:.3f} 股**
- 報酬率最佳：**{best_profit_row['標的']}**（**{best_profit_row['報酬率(%)']:.2f}%**）
- 報酬率最弱：**{worst_profit_row['標的']}**（**{worst_profit_row['報酬率(%)']:.2f}%**）
"""
    )

    st.divider()

    # =============================
    # 圖表 1：走勢圖
    # =============================
    st.subheader("📊 加權指數與個股走勢圖")

    fig, ax1 = plt.subplots(figsize=(15, 6))

    for sid in ["5475", "3234", "3105", "3037", "0050"]:
        ax1.plot(
            data_map[sid].index,
            data_map[sid]["Close"],
            color=stock_color_map[sid],
            linewidth=2,
            label=stock_name_map[sid]
        )

    ax1.set_xlabel("Date")
    ax1.set_ylabel("")
    ax1.tick_params(axis="y", left=False, labelleft=False)
    ax1.set_yticks([])
    ax1.spines["left"].set_visible(False)
    ax1.spines["top"].set_visible(False)

    ax2 = ax1.twinx()
    ax2.plot(
        df_index.index,
        df_index["Close"],
        color=color_index,
        linewidth=2.4,
        label="加權指數 (TAIEX)"
    )
    ax2.set_ylabel("")
    ax2.tick_params(axis="y", right=False, labelright=False)
    ax2.set_yticks([])
    ax2.spines["right"].set_visible(False)
    ax2.spines["top"].set_visible(False)

    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left", ncol=2)

    fig.tight_layout()
    st.pyplot(fig)

    st.divider()

    # =============================
    # 圖表 2：累計漲跌幅圖
    # =============================
    st.subheader("🚀 累計漲跌幅對比圖")
    st.markdown("將各自起始日設為 **0%**，比較這段期間誰漲得多、誰波動比較大。")

    ret_map = {}
    for sid in ["5475", "3234", "3105", "3037", "0050", "TAIEX"]:
        ret_map[sid] = (data_map[sid]["Close"] / data_map[sid]["Close"].iloc[0] - 1) * 100

    df_compare_pct = pd.DataFrame({
        stock_name_map["5475"]: ret_map["5475"],
        stock_name_map["3234"]: ret_map["3234"],
        stock_name_map["3105"]: ret_map["3105"],
        stock_name_map["3037"]: ret_map["3037"],
        stock_name_map["0050"]: ret_map["0050"],
        stock_name_map["TAIEX"]: ret_map["TAIEX"]
    }).dropna()

    st.line_chart(
        df_compare_pct,
        color=[color_5475, color_3234, color_3105, color_3037, color_0050, color_index]
    )

    st.divider()

    # =============================
    # 圖表 3：相對大盤超額績效圖
    # =============================
    st.subheader("🎯 相對大盤超額績效圖")
    st.markdown("以 **加權指數 (TAIEX)** 為基準，觀察各標的相對於大盤的超額報酬表現。")

    excess_return_df = pd.DataFrame({
        "德宏 (5475) - 大盤": ret_map["5475"] - ret_map["TAIEX"],
        "光環 (3234) - 大盤": ret_map["3234"] - ret_map["TAIEX"],
        "穩懋 (3105) - 大盤": ret_map["3105"] - ret_map["TAIEX"],
        "欣興 (3037) - 大盤": ret_map["3037"] - ret_map["TAIEX"],
        "0050 - 大盤": ret_map["0050"] - ret_map["TAIEX"]
    }).dropna()

    st.line_chart(
        excess_return_df,
        color=[color_5475, color_3234, color_3105, color_3037, color_0050]
    )

    st.caption("0% 以上代表跑贏大盤，0% 以下代表落後大盤。")

    st.divider()

    # =============================
    # 相對大盤績效摘要表
    # =============================
    st.subheader("📋 相對大盤績效摘要表")

    summary_rows = []
    for sid in ["5475", "3234", "3105", "3037", "0050"]:
        summary_rows.append({
            "標的": stock_name_map[sid],
            "累計報酬 (%)": round(ret_map[sid].iloc[-1], 2),
            "同期大盤報酬 (%)": round(ret_map["TAIEX"].iloc[-1], 2),
            "超額績效 (%)": round((ret_map[sid] - ret_map["TAIEX"]).iloc[-1], 2)
        })

    summary_df = pd.DataFrame(summary_rows)

    def highlight_excess(val):
        try:
            val = float(val)
            if val > 0:
                return "color: red; font-weight: bold;"
            elif val < 0:
                return "color: green; font-weight: bold;"
            return ""
        except Exception:
            return ""

    st.dataframe(
        summary_df.style.format({
            "累計報酬 (%)": "{:.2f}",
            "同期大盤報酬 (%)": "{:.2f}",
            "超額績效 (%)": "{:.2f}"
        }).map(highlight_excess, subset=["超額績效 (%)"]),
        use_container_width=True
    )

    best_row = summary_df.loc[summary_df["超額績效 (%)"].idxmax()]
    worst_row = summary_df.loc[summary_df["超額績效 (%)"].idxmin()]

    st.markdown(
        f"""
**重點摘要**
- 跑贏大盤最多：**{best_row['標的']}**（超額績效 **{best_row['超額績效 (%)']:.2f}%**）
- 表現最弱：**{worst_row['標的']}**（超額績效 **{worst_row['超額績效 (%)']:.2f}%**）
"""
    )

    st.divider()

    # =============================
    # 法人近 10 天表
    # =============================
    st.subheader("🏦 近 10 天法人買賣超比較表")
    st.caption("5475、3234、3105、3037、0050、加權指數，各自獨立顯示")

    chip_start_date = (today - relativedelta(days=45)).strftime("%Y-%m-%d")
    chip_end_date = end_date

    raw_5475 = load_institutional_data("5475", chip_start_date, chip_end_date, is_market_total=False)
    raw_3234 = load_institutional_data("3234", chip_start_date, chip_end_date, is_market_total=False)
    raw_3105 = load_institutional_data("3105", chip_start_date, chip_end_date, is_market_total=False)
    raw_3037 = load_institutional_data("3037", chip_start_date, chip_end_date, is_market_total=False)
    raw_0050 = load_institutional_data("0050", chip_start_date, chip_end_date, is_market_total=False)
    raw_market = load_institutional_data(None, chip_start_date, chip_end_date, is_market_total=True)

    table_5475 = summarize_institutional_table(raw_5475, "德宏 (5475)", last_n_days=10)
    table_3234 = summarize_institutional_table(raw_3234, "光環 (3234)", last_n_days=10)
    table_3105 = summarize_institutional_table(raw_3105, "穩懋 (3105)", last_n_days=10)
    table_3037 = summarize_institutional_table(raw_3037, "欣興 (3037)", last_n_days=10)
    table_0050 = summarize_institutional_table(raw_0050, "0050", last_n_days=10)
    table_market = summarize_institutional_table(raw_market, "加權指數(大盤)", last_n_days=10)

    def prepare_chip_table(df):
        if df is None or df.empty:
            return pd.DataFrame(columns=["日期", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人買賣超"])

        show_df = df.copy().sort_values("日期").tail(10)
        show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")
        show_df = show_df[["日期", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人買賣超"]]
        return show_df

    chip_tables = {
        "德宏 (5475)": prepare_chip_table(table_5475),
        "光環 (3234)": prepare_chip_table(table_3234),
        "穩懋 (3105)": prepare_chip_table(table_3105),
        "欣興 (3037)": prepare_chip_table(table_3037),
        "0050": prepare_chip_table(table_0050),
        "加權指數(大盤)": prepare_chip_table(table_market),
    }

    chip_names = list(chip_tables.keys())
    for i in range(0, len(chip_names), 2):
        col1, col2 = st.columns(2)
        name1 = chip_names[i]
        with col1:
            st.markdown(f"### {name1}")
            if chip_tables[name1].empty:
                st.warning("無資料")
            else:
                st.dataframe(
                    style_net_table(chip_tables[name1]),
                    use_container_width=True,
                    height=420
                )

        if i + 1 < len(chip_names):
            name2 = chip_names[i + 1]
            with col2:
                st.markdown(f"### {name2}")
                if chip_tables[name2].empty:
                    st.warning("無資料")
                else:
                    st.dataframe(
                        style_net_table(chip_tables[name2]),
                        use_container_width=True,
                        height=420
                    )

    st.divider()

    # =============================
    # WantGoo 資料抓取
    # =============================
    st.subheader("🌐 WantGoo 籌碼資料")
    st.caption("同一次登入抓取：券商分點（近1日）＋大戶籌碼（近兩週）")

    if "wantgoo_results" not in st.session_state:
        st.session_state["wantgoo_results"] = None

    refresh_col1, refresh_col2 = st.columns([1, 6])
    with refresh_col1:
        refresh_clicked = st.button("重新抓取")
    with refresh_col2:
        st.caption("避免 Streamlit 每次重跑都重新啟動 Chrome，先存在 session_state。")

    if refresh_clicked or st.session_state["wantgoo_results"] is None:
        with st.spinner("正在抓取 WantGoo 券商分點與大戶籌碼資料..."):
            st.session_state["wantgoo_results"] = load_wantgoo_all_signals(
                WANTGOO_USERNAME,
                WANTGOO_PASSWORD,
                TARGET_STOCK_IDS,
                WANTGOO_HEADLESS
            )

    wantgoo_results = st.session_state["wantgoo_results"]

    if (not WANTGOO_USERNAME) or (not WANTGOO_PASSWORD):
        st.warning("⚠️ 尚未提供 WANTGOO_USERNAME / WANTGOO_PASSWORD，因此 WantGoo 相關資料目前會顯示『無資料』。")

    # =============================
    # 券商分點前三名
    # =============================
    st.subheader("🏛️ 券商分點買賣超前三名（近 1 日）")
    broker_top3_df = build_broker_top3_table(wantgoo_results)
    st.dataframe(broker_top3_df, use_container_width=True)

    st.divider()

    # =============================
    # 大戶籌碼比較表
    # =============================
    st.subheader("👥 大戶籌碼近兩週比較")
    major_holder_df = build_major_holder_table(wantgoo_results)

    def color_action(val):
        if val == "買":
            return "color: red; font-weight: bold;"
        elif val == "不買":
            return "color: green; font-weight: bold;"
        elif val == "觀望":
            return "color: orange; font-weight: bold;"
        return ""

    def color_major_delta(row):
        styles = [""] * len(row)
        try:
            now_v = row["最新一週大戶比例(%)"]
            prev_v = row["前一週大戶比例(%)"]
            if pd.notna(now_v) and pd.notna(prev_v):
                if now_v > prev_v:
                    styles[row.index.get_loc("最新一週大戶比例(%)")] = "color: red; font-weight: bold;"
                    styles[row.index.get_loc("前一週大戶比例(%)")] = "color: red;"
                elif now_v < prev_v:
                    styles[row.index.get_loc("最新一週大戶比例(%)")] = "color: green; font-weight: bold;"
                    styles[row.index.get_loc("前一週大戶比例(%)")] = "color: green;"
        except Exception:
            pass
        return styles

    st.dataframe(
        major_holder_df.style.format({
            "最新一週大戶比例(%)": "{:.2f}",
            "前一週大戶比例(%)": "{:.2f}",
        }).apply(color_major_delta, axis=1).map(
            color_action,
            subset=["大戶籌碼建議"]
        ),
        use_container_width=True
    )

    st.divider()

    # =============================
    # 融資 / 融券 / 借券
    # =============================
    st.subheader("💳 融資 / 融券 / 借券分析")
    st.caption("最近 20 天抓資料，表格顯示最近 7 個交易日，並以指數化方式把三條線放在同一張圖比較。")

    margin_data_map = load_all_margin_short_lending(
        TARGET_STOCK_IDS,
        margin_start_date,
        margin_end_date,
        FINMIND_TOKEN
    )

    margin_signal_summary_df = build_margin_signal_summary(margin_data_map)

    st.markdown("#### 📋 融資融券借券建議總表")
    st.dataframe(
        margin_signal_summary_df.style.format({
            "最新融資餘額": "{:,.0f}",
            "最新融券餘額": "{:,.0f}",
            "最新借券量": "{:,.0f}",
        }).map(
            color_action,
            subset=["融資融券借券建議"]
        ),
        use_container_width=True
    )

    st.markdown("#### 📈 各標的近一週融資 / 融券 / 借券走勢")

    for sid in TARGET_STOCK_IDS:
        label = stock_name_map_local.get(sid, sid)
        item = margin_data_map.get(sid, {})
        plot_df = item.get("df", pd.DataFrame())
        err = item.get("error")

        st.markdown(f"### {label}")

        if err:
            st.warning(f"{label}：{err}")
            continue

        if plot_df is None or plot_df.empty:
            st.warning(f"{label}：無資料")
            continue

        chart_col, table_col = st.columns([1.3, 1])

        with chart_col:
            fig_margin = plot_margin_short_lending_chart(plot_df, label)
            if fig_margin is not None:
                st.pyplot(fig_margin)

        with table_col:
            st.dataframe(
                prepare_margin_detail_table(plot_df).style.format({
                    "融資餘額": "{:,.0f}",
                    "融券餘額": "{:,.0f}",
                    "借券量": "{:,.0f}",
                }).map(
                    color_action,
                    subset=["建議"]
                ),
                use_container_width=True,
                height=320
            )

    st.markdown(
        """
**融資融券借券判讀規則**
- **不買**：當日融資餘額增加，且融券餘額減少
- **買**：當日借券量低於前一日
- **觀望**：其餘情況
- **無前日資料**：第一筆資料無法比較
"""
    )

    st.divider()

    # =============================
    # 買賣建議表
    # =============================
    st.subheader("🧠 建議買賣表（法人 + 券商分點 + 大戶籌碼 + 融資融券借券整合）")
    st.caption("法人規則：投信近三天買超 → 不買；外資與三大法人近三天皆賣超 → 不買；外資或三大法人近三天其中一項買超 → 買；否則觀望")

    recommendation_rows = [
        build_recommendation(table_5475, "德宏 (5475)"),
        build_recommendation(table_3234, "光環 (3234)"),
        build_recommendation(table_3105, "穩懋 (3105)"),
        build_recommendation(table_3037, "欣興 (3037)"),
        build_recommendation(table_0050, "0050"),
        build_recommendation(table_market, "加權指數(大盤)")
    ]
    recommendation_df = pd.DataFrame(recommendation_rows)

    broker_map = {
        "0050": wantgoo_results.get("0050", {}),
        "光環 (3234)": wantgoo_results.get("3234", {}),
        "德宏 (5475)": wantgoo_results.get("5475", {}),
        "穩懋 (3105)": wantgoo_results.get("3105", {}),
        "欣興 (3037)": wantgoo_results.get("3037", {})
    }

    margin_signal_map = {
        "0050": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "0050", "融資融券借券建議"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "0050"]) > 0 else "無資料",
        "光環 (3234)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "光環 (3234)", "融資融券借券建議"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "光環 (3234)"]) > 0 else "無資料",
        "德宏 (5475)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "德宏 (5475)", "融資融券借券建議"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "德宏 (5475)"]) > 0 else "無資料",
        "穩懋 (3105)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "穩懋 (3105)", "融資融券借券建議"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "穩懋 (3105)"]) > 0 else "無資料",
        "欣興 (3037)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "欣興 (3037)", "融資融券借券建議"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "欣興 (3037)"]) > 0 else "無資料",
    }

    margin_reason_map = {
        "0050": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "0050", "融資融券借券說明"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "0050"]) > 0 else "",
        "光環 (3234)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "光環 (3234)", "融資融券借券說明"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "光環 (3234)"]) > 0 else "",
        "德宏 (5475)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "德宏 (5475)", "融資融券借券說明"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "德宏 (5475)"]) > 0 else "",
        "穩懋 (3105)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "穩懋 (3105)", "融資融券借券說明"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "穩懋 (3105)"]) > 0 else "",
        "欣興 (3037)": margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "欣興 (3037)", "融資融券借券說明"].iloc[0]
        if len(margin_signal_summary_df.loc[margin_signal_summary_df["標的"] == "欣興 (3037)"]) > 0 else "",
    }

    recommendation_df["買超前三名"] = recommendation_df["標的"].map(
        lambda x: "、".join(broker_map.get(x, {}).get("buy_top3", [])) if broker_map.get(x, {}).get("buy_top3") else "-"
    )
    recommendation_df["賣超前三名"] = recommendation_df["標的"].map(
        lambda x: "、".join(broker_map.get(x, {}).get("sell_top3", [])) if broker_map.get(x, {}).get("sell_top3") else "-"
    )
    recommendation_df["券商分點建議"] = recommendation_df["標的"].map(
        lambda x: broker_map.get(x, {}).get("broker_signal", "-")
    )
    recommendation_df["券商分點說明"] = recommendation_df["標的"].map(
        lambda x: broker_map.get(x, {}).get("broker_reason", "-")
    )
    recommendation_df["大戶籌碼建議"] = recommendation_df["標的"].map(
        lambda x: broker_map.get(x, {}).get("major_signal", "-")
    )
    recommendation_df["大戶籌碼說明"] = recommendation_df["標的"].map(
        lambda x: broker_map.get(x, {}).get("major_reason", "-")
    )
    recommendation_df["融資融券借券建議"] = recommendation_df["標的"].map(
        lambda x: margin_signal_map.get(x, "-" if x != "加權指數(大盤)" else "-")
    )
    recommendation_df["融資融券借券說明"] = recommendation_df["標的"].map(
        lambda x: margin_reason_map.get(x, "-" if x != "加權指數(大盤)" else "-")
    )
    recommendation_df["綜合建議"] = recommendation_df.apply(
        lambda row: combine_suggestion(
            row.get("法人建議"),
            row.get("券商分點建議"),
            row.get("大戶籌碼建議"),
            row.get("融資融券借券建議")
        ),
        axis=1
    )

    def color_numeric(v):
        try:
            if pd.isna(v):
                return ""
            v = float(v)
            if v > 0:
                return "color: red; font-weight: bold;"
            elif v < 0:
                return "color: green; font-weight: bold;"
            return ""
        except Exception:
            return ""

    st.dataframe(
        recommendation_df.style.format({
            "近三天外資買賣超合計": "{:,.0f}",
            "近三天投信買賣超合計": "{:,.0f}",
            "近三天自營商買賣超合計": "{:,.0f}",
            "近三天三大法人買賣超合計": "{:,.0f}",
        }).map(
            color_action,
            subset=["法人建議", "券商分點建議", "大戶籌碼建議", "融資融券借券建議", "綜合建議"]
        ).map(
            color_numeric,
            subset=[
                "近三天外資買賣超合計",
                "近三天投信買賣超合計",
                "近三天自營商買賣超合計",
                "近三天三大法人買賣超合計",
            ]
        ),
        use_container_width=True
    )

    st.markdown(
        """
**判讀說明**
- **法人建議**
  - **不買**：投信近三天買超
  - **不買**：外資與三大法人近三天皆為賣超
  - **買**：外資近三天買超，或三大法人近三天買超
  - **觀望**：以上條件都未成立

- **券商分點建議**
  - **買**：買超前三名中出現摩根大通 / 摩根士丹利，且未出現明顯偏空條件
  - **不買**：買超前三名中指定偏空券商達 2 家以上，或賣超前三名中出現摩根大通 / 摩根士丹利
  - **觀望**：多空訊號同時出現，或無明確訊號

- **大戶籌碼建議**
  - **買**：最新一週大戶比例高於前一週
  - **不買**：最新一週大戶比例低於前一週
  - **觀望**：兩週比例持平
  - **無資料**：頁面元素或資料未成功抓取

- **融資融券借券建議**
  - **不買**：融資增加且融券減少
  - **買**：借券量低於前一日
  - **觀望**：其餘情況
  - **無資料**：資料抓取失敗或不足

- **綜合建議**
  - 任一方出現 **不買** → **不買**
  - 否則只要任一方出現 **買** → **買**
  - 其餘 → **觀望**
"""
    )

    st.divider()


# --- 第二部分：啟動邏輯 ---
if __name__ == "__main__":
    if os.environ.get("STREAMLIT_ALREADY_RUNNING") == "true":
        run_streamlit_app()
    else:
        target_port = "8888"
        target_host = "127.0.0.1"

        new_env = os.environ.copy()
        new_env["STREAMLIT_ALREADY_RUNNING"] = "true"

        file_path = os.path.abspath(__file__)

        process = subprocess.Popen(
            [
                sys.executable, "-m", "streamlit", "run", file_path,
                "--server.headless", "true",
                "--server.port", target_port,
                "--server.address", target_host
            ],
            env=new_env
        )

        time.sleep(3)
        webbrowser.open(f"http://{target_host}:{target_port}")

        try:
            process.wait()
        except KeyboardInterrupt:
            process.terminate()
            print("服務已停止。")
