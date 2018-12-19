from sanic import Sanic
from sanic.response import json
import requests
import pyotp
import time
import json as pyjson
from kafka import KafkaProducer

app = Sanic('samaritan')
app.config.from_envvar('SAMARITAN_SETTINGS')

secrate = 'OFVAJG4SOB3RKGLS'
totp = pyotp.TOTP(secrate)

SMS_TEMPLATE = '''Dear {0}, we're looking forward to hosting you at {1}.

Address: {2}, {3}

Hotel Contact Number: {4}

Google Map directions: {5}
'''

producer = KafkaProducer(bootstrap_servers = app.config.KAFKA_BROKERS)

# print(app.config.DB_HOST)

while True:
    print(totp.now())
    time.sleep(2)

def createResponse(success=False, data={}, message=''):
    ret = {
        "success": success,
        "data": data,
        "message": message
    }
    return ret

def bookingStatusGenerator(code):
    bookingType = {
        0: 'Tentative',
        1: 'Confirmed'
    }

    return bookingType.get(code, 'Unknown')

def urlGenerator(type='', endPoint=''):
    typeList = {
        'booking': app.config.FAB_BOOKING_URL,
        'catalog': app.config.FAB_CATALOG_URL
    }
    endPointList = {
        'detail': 'booking/elastic/search',
        'propDetail': 'property/detail'
    }
    return (typeList.get(type, '') + endPointList.get(endPoint, ''))

def validateRequest(otp):
    otpTokenHistoryList = [totp.now(), totp.at(time.time()-30), totp.at(time.time()-60)]

    if (1==2): #str(otp) not in otpTokenHistoryList:
        return json(createResponse(message='OTP Expired, Please provide a valid OTP'))
    else:
        return True

def requestSenatizer(request):
    try:
        mobile = request.json.get('mobile', '')
        otp = request.json.get('otp', '')
    except Exception as e:
        return json(createResponse(message='Request is not in a valid format, Please use json to send data'))

    if mobile is None:
        return json(createResponse(message='Mobile no can\'t be null'))

    if otp is None:
        return json(createResponse(message='OTP can\'t be null'))

    return mobile, otp

@app.route("/fetch/booking_details", methods=['POST'])
async def fetchLatestBookingDetails(request):
    mobile, otp = requestSenatizer(request)

    if validateRequest(otp):
        url = urlGenerator(type='booking', endPoint='detail')
        data={
            "limit": 1,
            "mobile": mobile,
            "pageNo": 1,
            "sortOrder": "DESC",
            "sortingField": "id"
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        response = requests.request('POST', url, data=pyjson.dumps(data),headers=headers)
        jsonResponse = response.json()
        
        responseStatus = jsonResponse.get('status', False)

        if responseStatus:
            responseData = pyjson.loads(jsonResponse.get('data', '"[]"'))
            responseData = responseData[0]
            finalResponse = {
                "propertyName": responseData['propertyName'],
                "userFirstName": responseData['userDetails']['firstName'],
                "userLastName": responseData['userDetails']['lastName'],
                "travellerFirstName": responseData['travellerDetails'][0]['firstName'],
                "travellerLastName": responseData['travellerDetails'][0]['lastName'],
                "checkin": responseData['checkin'],
                "checkout": responseData['checkout'],
                "orderNo": responseData['order_no'],
                "otaBookingId": ''
            }

            if (responseData['order_no'] != responseData['ota_booking_id']):
                finalResponse['otaBookingId'] = responseData['ota_booking_id']

            finalResponse['bookingStatus'] = bookingStatusGenerator(responseData['booking_status'])
            return json(createResponse(True, finalResponse))
        else:
            return json(createResponse(message='Sorry, we are unable to connect with booking service. Please try after some time'))

@app.route("/send/location_sms", methods=['POST'])
async def sendLocationSms(request):

    mobile, otp = requestSenatizer(request)
    if validateRequest(otp):
        url = urlGenerator(type='booking', endPoint='detail')
        data={
            "limit": 1,
            "mobile": mobile,
            "pageNo": 1,
            "sortOrder": "DESC",
            "sortingField": "id"
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        response = requests.request('POST', url, data=pyjson.dumps(data),headers=headers)
        jsonResponse = response.json()
        
        responseStatus = jsonResponse.get('status', False)
        responseData = pyjson.loads(jsonResponse.get('data', '"[]"'))
        responseData = responseData[0]
        finalResponse = {
                "travellerFirstName": responseData['travellerDetails'][0]['firstName'],
                "travellerLastName": responseData['travellerDetails'][0]['lastName']
            }

        if responseStatus:
            propertyId = responseData['property_id']
            url = urlGenerator(type='catalog', endPoint='propDetail')
            data={
                "propertyId": propertyId
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
            response = requests.get(url, params=data, headers=headers)
            jsonResponse = response.json()
            
            responseStatus = jsonResponse.get('status', False)
            responseData = jsonResponse.get('data', {})

            finalResponse.update({
                "propertyName": responseData['propertyName'],
                "propertyAddress": responseData['propertyAddress'],
                "propertyCity": responseData['propertyCity'],
                "propertyPhoneNo": responseData['propertyPhoneNo'],
                "propertyMap": responseData['propertyMap']
            })

            print(finalResponse)

            finalSms = SMS_TEMPLATE.format(finalResponse['travellerFirstName'], 
                    finalResponse['propertyName'],
                    finalResponse['propertyAddress'],
                    finalResponse['propertyCity'],
                    finalResponse['propertyPhoneNo'],
                    finalResponse['propertyMap']
                )

            m = {
                'mobile': mobile,
                'countryCode': '+91',
                'text': finalSms,
                'smsType': 'TEXT'
            }
            print(m)

            future = producer.send(app.config.KAFKA_TOPIC, pyjson.dumps(m).encode('utf-8'))
            producer.flush()

            if responseStatus:
                return json(createResponse(True, finalSms))
            else:
                return json(createResponse(message='Sorry, we are unable to connect with booking service. Please try after some time'))

if __name__ == "__main__":
    app.run(host=app.config.HOST, port=app.config.PORT, workers=app.config.WORKERS)
