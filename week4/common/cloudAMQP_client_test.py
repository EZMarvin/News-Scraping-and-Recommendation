from cloudAMQP_client import CloudAMQPClient

CLOUDAMQP_URL = 'amqp://dfrwrfgh:57HQ4sghISj3dAGA42BQbVf9AOqzrj0c@crocodile.rmq.cloudamqp.com/dfrwrfgh'
QUEUE_NAME = 'test'

def test_basic():
    client = CloudAMQPClient(CLOUDAMQP_URL, QUEUE_NAME)

    sentMsg = {'test_key': 'value'}
    client.sendMessage(sentMsg)
    client.sleep(5)
    receivedMsg = client.getMessage()
    assert sentMsg == receivedMsg
    print 'test passed'

if __name__ == "__main__":
    test_basic()
