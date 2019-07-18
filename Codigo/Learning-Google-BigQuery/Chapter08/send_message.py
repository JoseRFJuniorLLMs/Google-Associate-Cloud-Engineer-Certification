import argparse
import os
from google.cloud import pubsub

def publish_messages(project, topic_name,message):

    #create publisher client
    publisher = pubsub.PublisherClient()

    #create a topic
    topic_path = publisher.topic_path(project, topic_name)

    #pass the message to data for further encoding process
    data = u'Message : {}'.format(message)

    # Data must be a bytestring
    data = data.encode('utf-8')

    publisher.publish(topic_path, data=data,employee_id='1',first_name='vijay',last_name='rajarathinam',date_of_joining='2016-04-19',country='INDIA')

    print('Published messages.')


if __name__ == '__main__':

    # set os environment for google credentials same as above.
    # export GOOGLE_APPLICATION_CREDENTIALS="/home/vijayr/PycharmProjects/cloud/cloudpocproject-4a434e16519d.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getcwd()+"/cloudpocproject-4a434e16519d.json"

    #assign project name, topic name , subscriber name
    project='cloudpocproject-178321'
    subscription_name = 'subcriber1'
    topic_name = 'my-new-topic'

    #message to be published
    message='New Employee added';

    #call for function
    publish_messages(project, topic_name,message)
