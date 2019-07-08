import time
import os
import big_query
from google.cloud import pubsub,bigquery


def receive_messages(project, subscription_name):
    """Receives messages from a pull subscription."""
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription_name)

    # get the bigquery client
    bqclient = bigquery.Client.from_service_account_json(os.getcwd() + '/ProdProject.json')

    def callback(message):
        # print message
        print('Received message: {}'.format(message))

        #add the inserted message to bigquery
        big_query.insert_pubsub_messages(
            bqclient,
            message,
            employee_id = message.attributes['employee_id'],
            first_name = message.attributes['first_name'],
            last_name = message.attributes['last_name'],
            date_of_joining = message.attributes['date_of_joining'],
            country = message.attributes['country']
        )

        # send acknoledgement to pub sub that message is recived
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)

if __name__ == '__main__':

    #export GOOGLE_APPLICATION_CREDENTIALS="/home/vijayr/PycharmProjects/cloud/cloudpocproject-4a434e16519d.json"
    #set os environment for google credentials same as above.
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getcwd()+"/cloudpocproject-4a434e16519d.json"

    #assign  project name , topic name , subscriber name
    project='cloudpocproject-178321'
    subscription_name='subcriber1'
    topic_name = 'my-new-topic'

    #call for recive messages
    receive_messages(project, subscription_name)