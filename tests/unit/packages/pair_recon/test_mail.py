from pair_recon.mail import send_outlook


class Attachments:
    def __init__(self):
        self.paths = []

    def Add(self, path):
        self.paths.append(path)


class Message:
    def __init__(self):
        self.Attachments = Attachments()
        self.saved = False
        self.sent = False

    def Save(self):
        self.saved = True

    def Send(self):
        self.sent = True


class Outlook:
    def __init__(self):
        self.message = Message()

    def CreateItem(self, kind):
        assert kind == 0
        return self.message


class Client:
    def __init__(self):
        self.outlook = Outlook()

    def Dispatch(self, name):
        assert name == "Outlook.Application"
        return self.outlook


def test_send(tmp_path):
    client = Client()
    attachment = tmp_path / "左侧.log"
    attachment.write_text("完成", encoding="utf-8")
    result = send_outlook(
        "a@example.com;b@example.com",
        "Done",
        "Body",
        [attachment],
        client=client,
    )
    message = client.outlook.message
    assert result["status"] == "sent"
    assert message.To == "a@example.com; b@example.com"
    assert message.saved and message.sent
    assert message.Attachments.paths == [str(attachment.resolve())]
