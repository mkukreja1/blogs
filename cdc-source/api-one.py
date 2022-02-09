from flask import Flask, Response, stream_with_context, jsonify
import time
from datetime import datetime
import calendar
import random


APP = Flask(__name__)


@APP.route("/hotel/<int:rowcount>", methods=["GET"])
def get_large_request(rowcount):
    """retunrs N rows of data"""
    result = {}
    def f():
        """The generator of mock data"""
        for _i in range(rowcount):
            time.sleep(.01)
            p_timestamp = calendar.timegm(datetime.utcnow().timetuple())
            locations = ['TORONTO, CANADA','OTTAWA, CANADA', 'MONTREAL, CANADA', 'NEW YORK, USA', 'LOS ANGELES, USA', 'LONDON, UK']
            city = random.choice(locations)
            hotels = ['MARIOTT','HILTON', 'CHOICE', 'WYNDHAM']
            hotel = random.choice(hotels)
            price = round(random.uniform(200, 350), 0)
            payload = {
                "timestamp": p_timestamp,
                "city": city,
                "hotel": hotel,
                "price": price
              }
            yield f"{payload}"
    return Response(stream_with_context(f()), status=201, mimetype='application/json')   
    #return Response(stream_with_context(f()))


if __name__ == "__main__":
    APP.run(port=8001,debug=True)
