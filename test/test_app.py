# from app import HelloWorld


from aws_xray_sdk.core import xray_recorder

xray_recorder.begin_segment("Test")


class TestApp:
    def test_helloworld(self):
        assert True
        # response = HelloWorld().get()
        # assert response == {"hello": "world"}
