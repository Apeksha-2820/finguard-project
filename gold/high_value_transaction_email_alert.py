from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ── Email Configuration ──────────────────────────────────────────────
# TODO: Move credentials to a Databricks secret scope for production use:
#   1. Create scope:  dbutils.secrets.create_scope("finguard")
#   2. Add the key:   dbutils.secrets.put(scope="finguard", key="gmail_app_password", value=<password>)
#   3. Reference it:  APP_PASSWORD = dbutils.secrets.get(scope="finguard", key="gmail_app_password")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "apekshasarode28@gmail.com"
APP_PASSWORD = "mgec xiww sctq zixy"  # Gmail App Password — move to dbutils.secrets before production


def _build_alert_email_body(row: dict) -> str:
    """Build an HTML email body for a single high-value transaction alert."""
    return f"""
<html>
<body>

<h2>⚠️ High Value Transaction Alert</h2>

<p>Dear {row['customer_name']},</p>

<p>A transaction on your account has exceeded your configured transaction limit.</p>

<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
  <tr><td><b>Transaction ID</b></td><td>{row['transaction_id']}</td></tr>
  <tr><td><b>Amount</b></td><td>{row['transaction_amount']} {row['currency']}</td></tr>
  <tr><td><b>Transaction Limit</b></td><td>{row['transaction_limit']}</td></tr>
  <tr><td><b>Merchant</b></td><td>{row['merchant_name']} ({row['merchant_category']})</td></tr>
  <tr><td><b>Timestamp</b></td><td>{row['transaction_timestamp']}</td></tr>
</table>

<br>
<p>If you do not recognise this transaction, please contact FinGuard Support immediately.</p>

<p>
Thanks<br>
<b>FinGuard Support Team</b>
</p>

</body>
</html>
"""


def _send_alert_email(recipient: str, subject: str, body: str) -> None:
    """Send a single email via Gmail SMTP."""
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.send_message(msg)
        print(f"✅ Email sent to {recipient}")
    except Exception as e:
        print(f"❌ Error sending email to {recipient}: {e}")


def _send_alerts_batch(df: DataFrame, batch_id: int) -> None:
    """foreachBatch callback — iterate over alert rows and email each customer."""
    if df.isEmpty():
        return

    for row in df.collect():
        alert_data = {
            "transaction_id": row["transaction_id"],
            "customer_name": row["customer_name"],
            "transaction_amount": row["transaction_amount"],
            "transaction_limit": row["transaction_limit"],
            "currency": row["currency"],
            "merchant_name": row["merchant_name"],
            "merchant_category": row["merchant_category"],
            "transaction_timestamp": row["transaction_timestamp"],
        }
        subject = f"⚠️ High Value Transaction Alert — Transaction {row['transaction_id']}"
        body = _build_alert_email_body(alert_data)
        _send_alert_email(row["customer_email"], subject, body)


# ── Sink: send email alerts for each high-value transaction ───────
# Note: the gold-layer detection table (finguard.gold.transactions) is defined
# separately in high_value_transactions_alert.py.
@dp.foreach_batch_sink(name="email_alert_sink")
def email_alert_sink(df, batch_id):
    """ForEachBatch sink — iterate over alert rows and email each customer."""
    _send_alerts_batch(df, batch_id)


@dp.append_flow(target="email_alert_sink")
def send_high_value_email_alerts():
    """Read high-value transaction alerts and route them to the email sink."""
    return spark.readStream.table("finguard.gold.transactions")
