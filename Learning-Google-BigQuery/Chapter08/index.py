from google.cloud import bigquery,pubsub
import time
import os
# def run_quickstart():
#     # [START pubsub_quickstart]
#     publisher = pubsub.PublisherClient()
#     topic_path = publisher.topic_path(
#         'cloudpocproject-178321', 'my-new-topic')
#
#     # Create the topic.
#     topic = publisher.create_topic(topic_path)
#
#     print('Topic created: {}'.format(topic))
    # [END pubsub_quickstart]

def publish_messages(project, topic_name):
    """Publishes multiple messages to a Pub/Sub topic."""
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    for n in range(1, 10):
        data = u'Message number {}'.format(n)
        # Data must be a bytestring
        data = data.encode('utf-8')
        publisher.publish(topic_path, data=data)

    print('Published messages.')

def receive_messages(project, subscription_name):
    """Receives messages from a pull subscription."""
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        print('Received message: {}'.format(message))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)


if __name__ == '__main__':
    # run_quickstart()

    #export GOOGLE_APPLICATION_CREDENTIALS="/home/vijayr/PycharmProjects/cloud/cloudpocproject-4a434e16519d.json"
    #set os environment for google credentials same as above.
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getcwd()+"/cloudpocproject-4a434e16519d.json"

    #assign topic name , subscriber name , project name
    project,subscription_name,topic_name = 'cloudpocproject-178321','subcriber1','my-new-topic'
    # publish_messages(project, topic_name)
    receive_messages(project, subscription_name)