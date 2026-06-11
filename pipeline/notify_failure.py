#!/usr/bin/env python3
"""管道失败时发邮件告警(复用周报系统的 Gmail SMTP 模式)。"""
import os, smtplib, datetime
from email.mime.text import MIMEText

user, pwd = os.environ.get("GMAIL_USER"), os.environ.get("GMAIL_APP_PASSWORD")
if not (user and pwd):
    raise SystemExit("no gmail creds, skip notify")
msg = MIMEText(f"macro-topology 数据管道运行失败 @ {datetime.datetime.utcnow():%Y-%m-%d %H:%M} UTC\n请查看 GitHub Actions 日志。", "plain", "utf-8")
msg["Subject"] = "[ALERT] macro-topology data pipeline failed"
msg["From"] = user
msg["To"] = user
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
    s.login(user, pwd)
    s.send_message(msg)
print("alert sent")
