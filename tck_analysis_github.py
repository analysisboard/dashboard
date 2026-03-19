import yfinance as yf
import pandas as pd
import pandas_ta as ta

# 분석할 종목
tickers = [

    
"XLK","XLV","XLF","XLE","XLI","XLY","XLP","XLU","XLRE","XLC","XLB","SOXX","IGV","SKYY",
"HACK","ROBO","BOTZ","IBB","ARKG","ARKF","BLOK","QTUM","GRID","ICLN","TAN","FAN","URA","LIT","BATT",
"GLD","GDX","SIL","COPX","REMX","PALL","MOO","PHO","WOOD","DBC","ITA","XAR","JEDI","JETS","UFO",
"VWO","MCHI","EWZ","FLIN","FLTW","ITB","SRVR","SLX","UUP","SHY","IEF","TLT","IBIT","ETHA","IWB","VHT",

"AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","BRK-B","TSLA","AVGO",
"LLY","JPM","V","MA","XOM","UNH","JNJ","PG","HD","COST",
"ABBV","MRK","PEP","KO","ADBE","NFLX","CRM","ACN","WMT","CSCO",
"ABT","TMO","MCD","LIN","BAC","AMD","ORCL","CMCSA","DIS","TXN",
"INTU","QCOM","NEE","HON","UNP","AMGN","IBM","LOW","SBUX","PM",
"INTC","GS","RTX","SPGI","BLK","CAT","ISRG","PLD","AMAT","BKNG",
"MDT","GE","GILD","ADI","MDLZ","AXP","CVX","LMT","NOW","T",
"ADP","C","MO","ZTS","DE","MMC","SO","VRTX","PGR","REGN",
"BDX","SYK","ELV","DHR","SHW","CI","EOG","ITW","SLB","DUK",
"CL","HCA","ICE","FIS","APD","AON","WM","PSA","KLAC","NSC",
"CSX","NOC","EQIX","GD","ETN","PNC","HUM","USB","FCX","BSX",
"MU","MCK","ORLY","ROP","SNPS","ADSK","EW","NXPI","MAR","FTNT",
"AZO","PH","ECL","PAYX","SPG","EMR","CTAS","AEP","WELL","OXY",
"MNST","KMB","TEL","MSI","PRU","CME","AIG","TRV","ALL","AMP",
"AJG","AFL","BK","COF","DFS","FITB","HBAN","KEY","RF","STT",
"TFC","MTB","SCHW","CB","NTRS","WRB","SIVB","BEN","FRC","CBOE",
"IVZ","PFG","CFG","L","MET","RJF","AIZ","UNM","PRI","BRO",
"ARE","AVB","BXP","CBRE","CCI","DLR","EQIX","ESS","EXR","FRT",
"HST","IRM","KIM","MAA","O","PLD","PSA","REG","SBAC","SPG",
"UDR","VTR","VICI","WELL","WY","AMT","CROWN","SBA","DLTR","DG",
"TGT","ROST","TJX","KSS","ULTA","BBY","DHI","LEN","NVR","PHM",
"POOL","TPR","RCL","CCL","NCLH","LVS","WYNN","MGM","EXPE","BKNG",
"ABNB","UAL","DAL","AAL","LUV","ALK","UPS","FDX","CHRW","JBHT",
"ODFL","EXPD","CSX","NSC","UNP","KSU","CP","CNI","WAB","TRN",
"PCAR","CMI","DE","CAT","ITW","PH","ROK","ETN","EMR","DOV",
"IR","XYL","IEX","AME","TT","JCI","MAS","AOS","NDSN","PNR",
"FAST","GWW","SNA","SWK","WHR","LII","FBHS","LEG","TPX","NWL",
"CLX","CHD","KMB","PG","CL","CAG","CPB","GIS","K","SJM",
"HSY","MDLZ","PEP","KO","MNST","STZ","TAP","BF-B","KDP","EL",
"ESTEE","ULTA","COTY","REV","NKE","VFC","PVH","RL","TPR","HBI",
"COLM","LULU","DECK","CROX","SBUX","YUM","YUMC","CMG","DPZ","DRI",
"QSR","MCD","WEN","JACK","BWLD","CAKE","TXRH","EAT","PLAY","RRGB",
"KR","WMT","COST","TGT","DG","DLTR","BJ","ACI","SFM","CASY",
"KHC","TSN","HRL","CPB","CAG","GIS","SJM","MKC","LW","BG",
"ADM","MOS","CF","NTR","FMC","CTVA","ALB","LYB","DOW","DD",
"PPG","SHW","APD","ECL","LIN","IFF","CE","HUN","OLN","AXTA",
"NEM","GOLD","FCX","SCCO","VALE","RIO","BHP","TECK","MP","AA",
"HAL","SLB","BKR","NOV","FANG","EOG","PXD","DVN","MRO","APA",
"XOM","CVX","COP","PSX","VLO","MPC","OXY","KMI","WMB","OKE",
"ENB","TRP","PPL","DUK","SO","AEP","EXC","PEG","ED","EIX",
"D","PCG","SRE","NEE","FE","NRG","AES","CEG","ATO","LNT",
"AEE","CMS","DTE","EVRG","ES","WEC","XEL","IDA","OGE","PNW",
"POR","BKH","ALE","HE","AVA","MGEE","NWE","OTTR","PNM","SJI"

					
                                     
]

# -------------------------------
# 유틸 함수
# -------------------------------

def fix_columns(df):
    """yfinance MultiIndex 방어"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def calc_return(df, days):
    """지정된 기간(거래일 기준) 동안의 수익률 계산"""
    if len(df) < (days + 1):
        return None
    price_now = df['Close'].iloc[-1]
    price_prev = df['Close'].iloc[-(days + 1)]
    return ((price_now / price_prev) - 1) * 100

def cross_signal(df):
    """골든/데드 크로스 여부"""
    if len(df) < 200:
        return None
    sma50 = df['Close'].rolling(50).mean()
    sma200 = df['Close'].rolling(200).mean()
    return "Golden" if sma50.iloc[-1] > sma200.iloc[-1] else "Dead"

def dist_ma(df, period):
    """이평선 이격도"""
    if len(df) < period:
        return None
    price = df['Close'].iloc[-1]
    ma = df['Close'].rolling(period).mean().iloc[-1]
    return ((price / ma) - 1) * 100

def calc_rsi(df):
    """RSI(14) 계산"""
    rsi = ta.rsi(df['Close'], length=14)
    return float(rsi.iloc[-1]) if rsi is not None and not rsi.empty else None

def volume_change(df):
    """거래량 증감률"""
    if len(df) < 20:
        return None
    vol_now = df['Volume'].iloc[-1]
    vol_avg = df['Volume'].rolling(20).mean().iloc[-1]
    return ((vol_now / vol_avg) - 1) * 100

# -------------------------------
# 메인 분석
# -------------------------------

results = []

for ticker in tickers:
    print(f"분석중: {ticker}")

    try:
        # 1. 시가총액 정보 가져오기
        info = yf.Ticker(ticker).info
        market_cap = info.get('marketCap', None)
        # 10억 달러(Billion) 단위로 변환
        market_cap_b = market_cap / 1e9 if market_cap else None

        # 2. 가격 데이터 다운로드
        df_d = yf.download(ticker, period="2y", interval="1d", progress=False)
        df_w = yf.download(ticker, period="10y", interval="1wk", progress=False)
        df_m = yf.download(ticker, period="20y", interval="1mo", progress=False)

        if df_d.empty or df_w.empty or df_m.empty:
            continue

        df_d, df_w, df_m = fix_columns(df_d), fix_columns(df_w), fix_columns(df_m)

        # ✨ [추가된 부분] NaN(결측치) 제거 로직
        # 중간에 비어있는 데이터나 아직 마감되지 않은 현재 주차/월의 빈 데이터를 날려줍니다.
        df_d.dropna(inplace=True)
        df_w.dropna(inplace=True)
        df_m.dropna(inplace=True)

        row = {
            "Ticker": ticker,
            "MarketCap($B)": market_cap_b,  # 시가총액 추가

            # 수익률
            "Return_1D": calc_return(df_d, 1),
            "Return_1W": calc_return(df_d, 5),
            "Return_1M": calc_return(df_d, 21),
            "Return_6M": calc_return(df_d, 126),
            "Return_1Y": calc_return(df_d, 252),

            # 크로스 시그널
            "Cross_D": cross_signal(df_d),
            "Cross_W": cross_signal(df_w),
            "Cross_M": cross_signal(df_m),

            # 이격도 (50MA)
            "Dist50_D": dist_ma(df_d, 50),
            "Dist50_W": dist_ma(df_w, 50),
            "Dist50_M": dist_ma(df_m, 50),

            # 이격도 (200MA)
            "Dist200_D": dist_ma(df_d, 200),
            "Dist200_W": dist_ma(df_w, 200),
            "Dist200_M": dist_ma(df_m, 200),

            # RSI
            "RSI_D": calc_rsi(df_d),
            "RSI_W": calc_rsi(df_w),
            "RSI_M": calc_rsi(df_m),

            # 거래량
            "Volume_D": volume_change(df_d),
            "Volume_W": volume_change(df_w),
            "Volume_M": volume_change(df_m)
        }
        results.append(row)

    except Exception as e:
        print(f"에러 발생 {ticker}: {e}")

# -------------------------------
# 결과 저장
# -------------------------------

df_result = pd.DataFrame(results)

# 소수점 2자리 반올림
df_result = df_result.round(2)

# 경로를 파일명만 남깁니다. (GitHub 작업용)
json_path = "stock_analysis.json"
df_result.to_json(json_path, orient="records")

print(f"저장 완료 : {json_path}")