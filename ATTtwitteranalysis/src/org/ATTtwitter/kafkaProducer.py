from kafka import KafkaProducer
from tweepy import OAuthHandler, API, StreamListener, Stream
#topic = "ATT-Twitter"
import json

class ATTListener(StreamListener):
    i = 0
    def on_connect(self):
        print("connected to stream server")
        
    def on_data(self, data):
        try:
            print(data)
            jdump = json.dumps(data, ensure_ascii = False)
            print(jdump)
            print("writing data to json file")
            prod.send("ATT-Twitter", key = self.i , value = jdump)
            self.i +=1 
        except BaseException as e:
            print("error on data")
            print(e)
    def on_error(self, status_code):
        print(status_code)
    def on_event(self, status):
        print(status)
    def on_disconnect(self, notice):
        print(notice)
        raise SystemExit
    def on_exception(self, exception):
        print(exception)
        raise SystemExit
    def on_status(self, status):
        print(status)
consumer_secret = 'ClNNtINxrwhAdNNqK3iVxKKP1i9qV0ToxQy0CMShvyyl8xR1xb'
consumer_key = '0A8eNy7FRsys2WuhdDg0lKII7'
access_token = '786706593635262464-tcrGyvu1totqJm4oeYoNsYY6YeT8hYM'
access_token_secret = 'EBaJkury41ILSmzJrWGgJzTsTsXMoXTTb5x5EL0reMtlc'

auth = OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = API(auth)
att = ATTListener()
prod = KafkaProducer(bootstrap_servers = ["localhost:9092"] ,linger_ms = 2, retries = 4)
if(prod == 420):
    raise SystemExit
twitter_stream = Stream(auth , att)
twitter_stream.filter(track=["#Data"])