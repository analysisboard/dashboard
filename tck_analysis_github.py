import yfinance as yf
import pandas as pd
import pandas_ta as ta

# 분석할 종목
tickers = [

    
"SPY","QQQ","DIA","XLK","XLV","XLF","XLE","XLI","XLY","XLP","XLU","XLRE","XLC","XLB","SOXX","IGV","SKYY",
"HACK","ROBO","BOTZ","IBB","ARKG","ARKF","BLOK","QTUM","GRID","ICLN","TAN","FAN","URA","LIT","BATT",
"GLD","GDX","SIL","COPX","REMX","PALL","MOO","PHO","WOOD","DBC","ITA","XAR","JEDI","JETS","UFO",
"VWO","MCHI","EWZ","FLIN","FLTW","ITB","SRVR","SLX","UUP","SHY","IEF","TLT","IBIT","ETHA","IWB","VHT",

"MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB","AKAM",
"ALB","ARE","ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN","AMCR","AEE","AEP","AXP","AIG","AMT",
"AWK","AMP","AME","AMGN","APH","ADI","AON","APA","APO","AAPL","AMAT","APP","APTV","ACGL","ADM","ARES","ANET",
"AJG","AIZ","T","ATO","ADSK","ADP","AZO","AVB","AVY","AXON","BKR","BALL","BAC","BAX","BDX","BRK-B","BBY","TECH",
"BIIB","BLK","BX","XYZ","BK","BA","BKNG","BSX","BMY","AVGO","BR","BRO","BF-B","BLDR","BG","BXP","CHRW","CDNS","CPT","CPB",
"COF","CAH","CCL","CARR","CVNA","CAT","CBOE","CBRE","CDW","COR","CNC","CNP","CF","CRL","SCHW","CHTR","CVX","CMG","CB","CHD",
"CIEN","CI","CINF","CTAS","CSCO","C","CFG","CLX","CME","CMS","KO","CTSH","COIN","CL","CMCSA","FIX","CAG","COP","ED","STZ","CEG","COO",
"CPRT","GLW","CPAY","CTVA","CSGP","COST","CTRA","CRH","CRWD","CCI","CSX","CMI","CVS","DHR","DRI","DDOG","DVA","DECK","DE","DELL","DAL",
"DVN","DXCM","FANG","DLR","DG","DLTR","D","DPZ","DASH","DOV","DOW","DHI","DTE","DUK","DD","ETN","EBAY","ECL","EIX","EW","EA","ELV","EME",
"EMR","ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ERIE","ESS","EL","EG","EVRG","ES","EXC","EXE","EXPE","EXPD","EXR",
"XOM","FFIV","FDS","FICO","FAST","FRT","FDX","FIS","FITB","FSLR","FE","FISV","F","FTNT","FTV","FOXA","FOX","BEN","FCX","GRMN",
"IT","GE","GEHC","GEV","GEN","GNRC","GD","GIS","GM","GPC","GILD","GPN","GL","GDDY","GS","HAL","HIG","HAS","HCA","DOC","HSIC","HSY",
"HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ","HUBB","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","INCY","IR","PODD","INTC",
"IBKR","ICE","IFF","IP","INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT","JBL","JKHY","J","JNJ","JCI","JPM","KVUE","KDP","KEY","KEYS",
"KMB","KIM","KMI","KKR","KLAC","KHC","KR","LHX","LH","LRCX","LW","LVS","LDOS","LEN","LII","LLY","LIN","LYV","LMT","L","LOW","LULU",
"LYB","MTB","MPC","MAR","MRSH","MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK","META","MET","MTD","MGM","MCHP","MU","MSFT",
"MAA","MRNA","MOH","TAP","MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP","NFLX","NEM","NWSA","NWS","NEE","NKE","NI",
"NDSN","NSC","NTRS","NOC","NCLH","NRG","NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS","PCAR","PKG","PLTR",
"PANW","PSKY","PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PNC","POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU",
"PEG","PTC","PSA","PHM","PWR","QCOM","DGX","Q","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY","HOOD","ROK","ROL","ROP","ROST",
"RCL","SPGI","CRM","SNDK","SBAC","SLB","STX","SRE","NOW","SHW","SPG","SWKS","SJM","SW","SNA","SOLV","SO","LUV","SWK","SBUX","STT","STLD",
"STE","SYK","SMCI","SYF","SNPS","SYY","TMUS","TROW","TTWO","TPR","TRGP","TGT","TEL","TDY","TER","TSLA","TXN","TPL","TXT","TMO","TJX","TKO",
"TTD","TSCO","TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UBER","UDR","ULTA","UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VLTO","VRSN",
"VRSK","VZ","VRTX","VTRS","VICI","V","VST","VMC","WRB","GWW","WAB","WMT","DIS","WBD","WM","WAT","WEC","WFC","WELL","WST","WDC","WY","WSM","WMB",
"WTW","WDAY","WYNN","XEL","XYL","YUM","ZBRA","ZBH","ZTS"

                                     
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

            # 이격도 (400MA)
            "Dist400_D": dist_ma(df_d, 400),
            "Dist400_W": dist_ma(df_w, 400),
            
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