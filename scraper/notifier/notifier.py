import base64
from dataclasses import dataclass
from email.mime.text import MIMEText
import os
import traceback
import logging

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError


from dotenv import load_dotenv

load_dotenv()
GOOGLE_SA_FILE = os.environ["GOOGLE_SA_FILE"]


@dataclass
class Notifier:
    recipient_email: str
    service_account_file: str = GOOGLE_SA_FILE

    def __post_init__(self):
        creds = Credentials.from_service_account_file(self.service_account_file)
        self.service = build("gmail", "v1", credentials=creds)

    def send_email(self, subject: str, body: str):
        message = MIMEText(body)
        message["to"] = self.recipient_email
        message["subject"] = subject
        create_message = {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}
        try:
            send_message = (
                self.service.users()
                .messages()
                .send(userId="me", body=create_message)
                .execute()
            )
            logging.info(
                f'Email was sent to {self.recipient_email} with id: {send_message["id"]}'
            )
        except HttpError as error:
            logging.error(f"An error occurred: {error}")
            send_message = None
        return send_message

    def notify_on_exception(self, func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                subject = "An exception has occurred"
                body = (
                    "An exception has occurred in your code:\n" + traceback.format_exc()
                )
                self.send_email(subject, body)

        return wrapper
