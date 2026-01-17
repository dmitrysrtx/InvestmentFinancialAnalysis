import time
import json
import yfinance as yf
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'financial_reports_stream'
# List of tech companies to analyze
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'INTC', 'NVDA', 'CSCO', 'ORCL', 'IBM', 'AMD']

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_financials(ticker):
    """
    Fetches financial data and historical price for a given ticker.
    Returns a list of dictionaries (one per year).
    """
    print(f"Fetching data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        
        # Fetch Balance Sheet and Income Statement
        balance_sheet = stock.balance_sheet
        financials = stock.financials
        # Fetch history for price labels
        history = stock.history(period="max")
        
        if balance_sheet.empty or financials.empty:
            return []

        data_points = []
        dates = financials.columns
        
        for date in dates:
            try:
                date_str = str(date.date())
                year = date.year
                
                # Helper to safely extract value from DF
                def get_val(df, key):
                    try:
                        return float(df.loc[key, date])
                    except KeyError:
                        return 0.0

                # Get stock price at the time of the report
                close_price = 0.0
                if date_str in history.index:
                    close_price = float(history.loc[date_str]['Close'])
                else:
                    # Fallback: average of that year
                    yearly_data = history[history.index.year == year]
                    if not yearly_data.empty:
                        close_price = float(yearly_data['Close'].mean())

                # Construct the data object based on your IMAGE requirements
                record = {
                    'ticker': ticker,
                    'year': year,
                    'report_date': date_str,
                    # --- Raw Values for Ratios ---
                    'total_revenue': get_val(financials, 'Total Revenue'),
                    'net_income': get_val(financials, 'Net Income'),
                    'current_assets': get_val(balance_sheet, 'Current Assets'),
                    'current_liabilities': get_val(balance_sheet, 'Current Liabilities'),
                    'total_assets': get_val(balance_sheet, 'Total Assets'),
                    'total_liabilities': get_val(balance_sheet, 'Total Liabilities Net Minority Interest'),
                    'stockholders_equity': get_val(balance_sheet, 'Stockholders Equity'),
                    'interest_expense': get_val(financials, 'Interest Expense'), # Needed for Coverage Ratio
                    'ebit': get_val(financials, 'EBIT'), # Earnings Before Interest & Taxes
                    # --- Price for Labeling ---
                    'close_price': close_price
                }
                data_points.append(record)
            except Exception as e:
                print(f"Skipping year {year} for {ticker}: {e}")
                
        return data_points
        
    except Exception as e:
        print(f"Failed to fetch {ticker}: {e}")
        return []

if __name__ == "__main__":
    print("--- Starting Data Ingestion ---")
    for ticker in TICKERS:
        reports = get_financials(ticker)
        for report in reports:
            # Sending to Kafka
            producer.send(TOPIC_NAME, value=report)
            print(f"Sent: {ticker} - {report['year']}")
            # Simulate real-time streaming delay
            time.sleep(0.2)
            
    producer.flush()
    print("--- Data Ingestion Complete ---")