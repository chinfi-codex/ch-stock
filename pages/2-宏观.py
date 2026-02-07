import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from tools.financial_data import EconomicIndicators

st.set_page_config(
    page_title="宏观经济指标",
    page_icon="📊",
    layout="wide"
)


# 商品数据部分 - 每个商品单独展示，三列布局
st.subheader("🛢️ 商品数据")

commodity_options = {
    'WTI原油': 'WTI',
    '天然气': 'NATURAL_GAS',
    '铜': 'COPPER',
}

# 获取所有商品数据
commodity_charts = []
for commodity_name, commodity_code in commodity_options.items():
    try:
        df = EconomicIndicators.get_commodities(commodity_code, interval='daily', curDate=200)
        if not df.empty:
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.sort_values('date')
            commodity_charts.append((commodity_name, df))
    except Exception as e:
        st.warning(f"获取{commodity_name}数据失败: {str(e)}")

# 三列布局展示商品图表
if commodity_charts:
    for i in range(0, len(commodity_charts), 3):
        cols = st.columns(3)
        for j, (commodity_name, df) in enumerate(commodity_charts[i:i+3]):
            with cols[j]:
                fig = px.line(
                    df,
                    x='date',
                    y='value',
                    title=commodity_name,
                    labels={'date': '日期', 'value': '价格'}
                )
                fig.update_layout(
                    height=300,
                    hovermode='x unified',
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)



# 第一行：汇率和国债收益率（一行三列）
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("💱 USD/CNY 汇率")
    try:
        usd_cny_daily = EconomicIndicators.get_exchangerates_daily('USD', 'CNY', curDate=200)
        if not usd_cny_daily.empty:
            usd_cny_daily = usd_cny_daily.copy()
            usd_cny_daily.index = pd.to_datetime(usd_cny_daily.index)
            usd_cny_daily.columns = ['汇率']
            usd_cny_daily = usd_cny_daily.sort_index()
            usd_cny_daily['汇率'] = pd.to_numeric(usd_cny_daily['汇率'], errors='coerce')
            
            fig = px.line(
                usd_cny_daily,
                x=usd_cny_daily.index,
                y='汇率',
                title='USD/CNY 汇率走势',
                labels={'index': '日期', '汇率': '汇率'}
            )
            fig.update_layout(
                height=300,
                hovermode='x unified',
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"获取汇率数据失败: {str(e)}")

with col2:
    st.subheader("📈 美国国债收益率")
    try:
        treasury_data = []
        for maturity in ['10year']:
            try:
                df = EconomicIndicators.get_treasury_yield(maturity=maturity, interval='daily', curDate=200)
                if not df.empty:
                    df['maturity'] = maturity
                    treasury_data.append(df)
            except:
                continue
        
        if treasury_data:
            combined_df = pd.concat(treasury_data, ignore_index=True)
            combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
            combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
            
            fig = go.Figure()
            for maturity in combined_df['maturity'].unique():
                maturity_df = combined_df[combined_df['maturity'] == maturity].sort_values('date')
                fig.add_trace(go.Scatter(
                    x=maturity_df['date'],
                    y=maturity_df['value'],
                    mode='lines+markers',
                    name=maturity.replace('year', '年期'),
                    line=dict(width=2)
                ))
            
            fig.update_layout(
                title='美国国债收益率走势对比',
                xaxis_title='日期',
                yaxis_title='收益率 (%)',
                height=300,
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"获取国债收益率数据失败: {str(e)}")

    with col3:
        st.subheader("🏦 美联储利率 (最近5年)")
        try:
            federal_rate = EconomicIndicators.get_federal_rate(interval='monthly')
            if not federal_rate.empty:
                federal_rate = federal_rate.copy()
                federal_rate['date'] = pd.to_datetime(federal_rate['date'], errors='coerce')
                federal_rate['value'] = pd.to_numeric(federal_rate['value'], errors='coerce')
                federal_rate = federal_rate.sort_values('date')
                
                # 筛选最近5年的数据
                five_years_ago = pd.Timestamp.now() - pd.DateOffset(years=5)
                federal_rate_5y = federal_rate[federal_rate['date'] >= five_years_ago]
                
                fig = px.line(
                    federal_rate_5y,
                    x='date',
                    y='value',
                    title='美联储利率历史走势',
                    labels={'date': '日期', 'value': '利率 (%)'}
                )
                fig.update_layout(
                    height=300,
                    hovermode='x unified',
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"获取美联储利率数据失败: {str(e)}")



# ---------------- 新增：中国宏观数据（TuShare） ----------------
st.divider()
st.subheader("中国宏观数据（TuShare）")

# 简单的辅助函数：挑选第一个可用的值列
def _pick_col(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    for col in df.columns:
        if str(col).lower() not in ['month', 'date', 'time', 'update_time', 'update_by', 'create_time', 'create_by', 'id']:
            return col
    return None


def _prep_two_year_df(df, date_col, value_col):
    """筛选今年+去年，并按月份透视，附带同比差值"""
    df = df[[date_col, value_col]].dropna()
    df[date_col] = pd.to_datetime(df[date_col], format='%Y%m', errors='coerce')
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    df = df.dropna(subset=[date_col, value_col])
    df['year'] = df[date_col].dt.year
    df['month_num'] = df[date_col].dt.month
    cur_year = datetime.now().year
    target_years = [cur_year - 1, cur_year]
    df = df[df['year'].isin(target_years)]
    if df.empty:
        return None, cur_year
    pivot = df.pivot_table(index='month_num', columns='year', values=value_col, aggfunc='first')
    # 保证两个年份都存在，便于差值计算
    for y in target_years:
        if y not in pivot.columns:
            pivot[y] = pd.NA
    pivot['yoy_diff'] = pivot[cur_year] - pivot[cur_year - 1]
    pivot = pivot.reset_index().sort_values('month_num')
    return pivot, cur_year


def _plot_two_year_with_yoy(title, pivot_df, cur_year):
    fig = go.Figure()
    prev_year = cur_year - 1
    if prev_year in pivot_df.columns:
        fig.add_trace(go.Scatter(
            x=pivot_df['month_num'],
            y=pivot_df[prev_year],
            mode='lines+markers',
            name=str(prev_year),
            line=dict(color='#6c757d')
        ))
    if cur_year in pivot_df.columns:
        fig.add_trace(go.Scatter(
            x=pivot_df['month_num'],
            y=pivot_df[cur_year],
            mode='lines+markers',
            name=str(cur_year),
            line=dict(color='#1f77b4')
        ))
    if 'yoy_diff' in pivot_df.columns:
        fig.add_trace(go.Bar(
            x=pivot_df['month_num'],
            y=pivot_df['yoy_diff'],
            name='同比增值',
            marker_color='#ff7f0e',
            opacity=0.35,
            yaxis='y2'
        ))

    fig.update_layout(
        title=title,
        xaxis=dict(title='月份', dtick=1, tickmode='linear'),
        yaxis=dict(title='数值'),
        yaxis2=dict(title='同比差值', overlaying='y', side='right', showgrid=False),
        height=340,
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    return fig

# 第一行：CPI, PPI, PMI
row1_col1, row1_col2, row1_col3 = st.columns(3)

with row1_col1:
    st.subheader("🥯 中国CPI")
    try:
        cpi_df = EconomicIndicators.get_cn_cpi(limit=120)
        if not cpi_df.empty:
            cpi_df = cpi_df.copy()
            month_col = 'month' if 'month' in cpi_df.columns else 'MONTH'
            value_col = _pick_col(cpi_df, ['nt_val', 'nt_yoy'])
            pivot_df, cur_year = _prep_two_year_df(cpi_df, month_col, value_col) if value_col else (None, None)
            if pivot_df is not None:
                fig = _plot_two_year_with_yoy("CPI：今年 vs 去年 + 同比增值", pivot_df, cur_year)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("未找到可绘制的CPI字段")
    except Exception as e:
        st.error(f"获取CPI数据失败: {str(e)}")

with row1_col2:
    st.subheader("🧰 中国PPI")
    try:
        ppi_df = EconomicIndicators.get_cn_ppi(limit=120)
        if not ppi_df.empty:
            ppi_df = ppi_df.copy()
            month_col = 'month' if 'month' in ppi_df.columns else 'MONTH'
            value_col = _pick_col(ppi_df, ['ppi_yoy', 'ppi_mom'])
            pivot_df, cur_year = _prep_two_year_df(ppi_df, month_col, value_col) if value_col else (None, None)
            if pivot_df is not None:
                fig = _plot_two_year_with_yoy("PPI：今年 vs 去年 + 同比增值", pivot_df, cur_year)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("未找到可绘制的PPI字段")
    except Exception as e:
        st.error(f"获取PPI数据失败: {str(e)}")

with row1_col3:
    st.subheader("🏭 中国PMI")
    try:
        pmi_df = EconomicIndicators.get_cn_pmi(limit=120)
        if not pmi_df.empty:
            pmi_df = pmi_df.copy()
            month_col = 'month' if 'month' in pmi_df.columns else 'MONTH'
            value_col = _pick_col(pmi_df, ['PMI010000', 'PMI030000'])
            pivot_df, cur_year = _prep_two_year_df(pmi_df, month_col, value_col) if value_col else (None, None)
            if pivot_df is not None:
                fig = _plot_two_year_with_yoy("PMI：今年 vs 去年 + 同比增值", pivot_df, cur_year)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("未找到可绘制的PMI字段")
    except Exception as e:
        st.error(f"获取PMI数据失败: {str(e)}")

# 第二行：M1, M2, 社融
row2_col1, row2_col2, row2_col3 = st.columns(3)

with row2_col1:
    st.subheader("💴 M1")
    try:
        money_df = EconomicIndicators.get_cn_money_supply(limit=120)
        if not money_df.empty:
            money_df = money_df.copy()
            month_col = 'month' if 'month' in money_df.columns else 'MONTH'
            money_df[month_col] = pd.to_datetime(money_df[month_col], format='%Y%m', errors='coerce')
            money_df = money_df.sort_values(month_col)
            if 'm1' in money_df.columns:
                money_df['m1'] = pd.to_numeric(money_df['m1'], errors='coerce')
                fig = px.line(
                    money_df,
                    x=month_col,
                    y='m1',
                    title="M1余额",
                    labels={month_col: '月份', 'm1': '亿元'}
                )
                fig.update_layout(height=320, hovermode='x unified', showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("未找到M1字段")
    except Exception as e:
        st.error(f"获取M1数据失败: {str(e)}")

with row2_col2:
    st.subheader("💴 M2 & 差值")
    try:
        money_df = EconomicIndicators.get_cn_money_supply(limit=120)
        if not money_df.empty:
            money_df = money_df.copy()
            month_col = 'month' if 'month' in money_df.columns else 'MONTH'
            money_df[month_col] = pd.to_datetime(money_df[month_col], format='%Y%m', errors='coerce')
            money_df = money_df.sort_values(month_col)
            if 'm2' in money_df.columns and 'm1' in money_df.columns:
                money_df['m2'] = pd.to_numeric(money_df['m2'], errors='coerce')
                money_df['m1'] = pd.to_numeric(money_df['m1'], errors='coerce')
                money_df['m_diff'] = money_df['m2'] - money_df['m1']
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=money_df[month_col], y=money_df['m2'], mode='lines', name='M2'))
                fig.add_trace(go.Scatter(x=money_df[month_col], y=money_df['m_diff'], mode='lines', name='M2-M1 差值'))
                fig.update_layout(
                    title="M2与差值",
                    xaxis_title='月份',
                    yaxis_title='亿元',
                    height=320,
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("未找到M1/M2字段")
    except Exception as e:
        st.error(f"获取M2数据失败: {str(e)}")

with row2_col3:
    st.subheader("🏦 社融增量")
    try:
        soci_df = EconomicIndicators.get_cn_soci(limit=120)
        if soci_df is not None and not soci_df.empty:
            soci_df = soci_df.copy()
            month_col = 'month' if 'month' in soci_df.columns else 'MONTH'
            soci_df[month_col] = pd.to_datetime(soci_df[month_col], format='%Y%m', errors='coerce')
            soci_df = soci_df.sort_values(month_col)
            value_col = _pick_col(soci_df, ['tsf', 'tsf_money', 'soci', 'tsf_yoy'])
            if value_col:
                soci_df[value_col] = pd.to_numeric(soci_df[value_col], errors='coerce')
                fig = px.line(
                    soci_df,
                    x=month_col,
                    y=value_col,
                    title="社会融资规模月度增量",
                    labels={month_col: '月份', value_col: '亿元'}
                )
                fig.update_layout(height=320, hovermode='x unified', showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("未找到可绘制的社融字段")
    except Exception as e:
        st.error(f"获取社融数据失败: {str(e)}")
