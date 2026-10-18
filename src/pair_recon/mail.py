from pathlib import Path


def send_outlook(recipients, subject: str, body: str, attachments=(), *, client=None):
    if isinstance(recipients, str):
        recipients = [
            value.strip() for value in recipients.replace(";", ",").split(",") if value.strip()
        ]
    if not recipients:
        return {"status": "skipped", "recipients": []}
    if client is None:
        try:
            import win32com.client
        except ImportError as exc:
            raise RuntimeError(
                "Outlook delivery requires Windows with pywin32 and Outlook"
            ) from exc

        client = win32com.client
    message = client.Dispatch("Outlook.Application").CreateItem(0)
    message.To = "; ".join(recipients)
    message.Subject = subject
    message.Body = body
    for attachment in attachments:
        message.Attachments.Add(str(Path(attachment).resolve()))
    message.Save()
    message.Send()
    return {"status": "sent", "recipients": recipients}
