import requests
import json
import datetime
from time import sleep


cookies = {
    '_gcl_au': '1.1.1738096459.1728315175',
    '_cs_ex': '1',
    '_cs_c': '1',
    '_tt_enable_cookie': '1',
    '_ttp': 'ZBw9mZHR2lVHTK9hokZMD8cOmDL',
    'tv-repeat-visit': 'true',
    '_gid': 'GA1.2.1118201711.1730736440',
    'tv_user': '{"authorizationLevel":100,"id":null}',
    'countryCode': 'HK',
    'ABTastySession': 'mrasn=',
    'cto_bundle': 'WL4BSl8yZXJtWGVDam5kQ2JTJTJCZnlZTnIyUyUyRmgyeUglMkJ1blg2bHpSUHprN3VScGtxdWVKTVdEVDhQeUZVWjlVWlFOdUw1MW5IT3lzU3dFZWhnZDRiTGpiajFWOUFkMmZrVGxvaElLZEpZS0FuRzFUOGxMcGJpcGJWclZtYlYzUGJmRDZ0a000JTJCbnlkNE9ydm1mTmEzQyUyRkdwY2lEVlQ4ZWQyaDhBZUtUZ0VQNEtoJTJCRnpZYzdVZVdqdyUyQmt3RUxMUzkyWUdaZUZNUWYlMkY0WEVwTXJsSXBTazdKTmVvUjNrc0QxVVUycVB3TFBrQmlyRmdvNjBqVXVzZEVaYlkxJTJCSVNBdllEVDFQ',
    'amp_f4354c': '1C-tjukTIS5uZmhG1dH9SQ...1ic02uidh.1ic031c6n.0.c.c',
    '_gat_UA-29776811-12': '1',
    'aws-waf-token': '6d56c36c-dcbb-45e7-aaa8-70419443591f:BgoAa5QtbDgaAAAA:a9TkoLvdoXfFv0pkGW+2mr4j4tf/gdU9oRigKHpDyImRhJVQ2kqpSfsZYLsQhFzw3vwanQL6kmvBOejfciC9VvDZvdu0vfFtwdz3J2/R4KW3KWLQSQ/qIi/Js2IGuc4A/o5ZVL0sDEKkc3hQ4zd9SLjqoppdgxQXuL5ziTfZN1/Ku8wykzs71UI9hx5wkPPEiVXi/Agw+b7+P9k5ITh6fS76RG71qwgku+NhwwU0aI9n0/vQuQDFeUQqPHyl674tU/PE',
    '_ga': 'GA1.2.717502229.1728315175',
    'amp_1a5adb': '3ocda_WnRYfz06paaF-Q1r...1ic02uidf.1ic031mju.vp.c.105',
    '_ga_RSRSMMBH0X': 'GS1.1.1730874920.40.1.1730875023.48.0.1047238187',
    '_dd_s': 'rum=0&expire=1730875923629&logs=1&id=df5e3426-3eaf-4533-b203-66b4d8953059&created=1730874918082',
    'tvl': '9U0Lu75ZwC0sfAB0fWdnrXZzcAjpDAntsijIM/pbIHXMQpzsefOSeD5yaGQhQ9lp1kNOHv9BqP1ZXI4hZFC1Rhjx713hC54NY84p//LtbQaRBA/W5N8uLKlsJpB2NJsxw72J5VFEF1G9O2C4TLxPHgbM7hgRwmbEUxSMN7JxMnowY2jVEETvoxvLMJNPPCl0zp4WErWI6lIw6t/Qpn7/h4oNqvtpQd8sfJOWeieo855OBm/jf0ZBQGOI6G4Ha+wsghLe/h0ft0w=~djAy',
    'tvs': 'Lg8Eqvkq4eaS51AAkifWdyPC9mraiFOczii99+XAqxWUfKZUzOExXIIwoQfdUETM46hoHZk2fkgPL3pQ5Wew2hVfMaGStJgHJTJT1EmWlsyNcBzCW866e9+NA9HRjKR+Gh6XAoiRXCX4t9QzJ6QtVaZem2UxSY2jd8ze6k5wsm9XAHCnar40zh5HQd2fqQv7Xmr9jAZy2bmKX7CMxBWJ/tU1TmCYGogMhK8qdwSAPeLrcpcIG2jk1+jIj83iZMcihc8Jlys5UpVOh1r5B6VnhAN0zBuUFbePbuM=~djAy',
}

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
    'content-type': 'application/json',
    # 'cookie': 'tv-repeat-visit=true; countryCode=VN; _gcl_aw=GCL.1727889328.Cj0KCQjw3vO3BhCqARIsAEWblcCDMxA0b7_cRRZLx9kPWSdSz7PLarzJZloyeOdgobwoOwIZh8p7McoaAnXdEALw_wcB; _gcl_gs=2.1.k1$i1727889326; _gcl_au=1.1.289440119.1727889328; _gid=GA1.2.146043254.1727889328; _gac_UA-29776811-12=1.1727889328.Cj0KCQjw3vO3BhCqARIsAEWblcCDMxA0b7_cRRZLx9kPWSdSz7PLarzJZloyeOdgobwoOwIZh8p7McoaAnXdEALw_wcB; _cs_ex=1; _cs_c=1; tv_user={"authorizationLevel":100,"id":null}; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22QfOXebp2v6yYCJqylMMt%22%7D; _fbp=fb.1.1727889361544.585527405763544454; _tt_enable_cookie=1; _ttp=SbGEq8YtjQ5LcmPmrfz-DH-VZA-; __rtbh.uid=%7B%22id%22%3A%22undefined%22%2C%22eventType%22%3A%22uid%22%7D; ABTastySession=mrasn=; cto_bundle=W5B-N19oVkw2UzIxUjlZem5VRnF5dEZaWnhJc2JkYkVuMTJTbHk5S2lIYXpNJTJCakVCTERySG45azNoSnd2MVJMS0pWejNYa0kzSkRMb1pwQ1RBeGclMkJKaWVDb0FvSllUOHVianVmYWwyRGNMOE00MFpXcGJwMWdyOWwwUGd4WW9jc0dXVyUyQkhqOVJrdDBsR3JDME9EYyUyQmJ0N2RUcW9FZmRYVDY2N2NzNHpGdmFpYldtR2Q2a05QZjFWQTlYclpqQ205M3BCblhrMlBLTmhlU3k1SVdVbU1PeVNmMXclM0QlM0Q; amp_f4354c=RDTULYUyphguS9jjPJOoqW...1i99r8mms.1i99rtpvr.0.8.8; _ga=GA1.2.2078792515.1727889328; amp_1a5adb=WLi-ISe5KcWt0CQV0tWm2v...1i99r06ld.1i99rv7hp.b9.8.bh; _ga_RSRSMMBH0X=GS1.1.1727980902.8.1.1727981928.2.0.0; aws-waf-token=a0b2db36-101a-426b-a80e-986d5dcf61c9:BgoAdziECSvIAAAA:8UxvniaW61z6jsaaglN5XsTRJg1Qpaa9zU2GZQ2eCY/Txaj9A2YFl+0hOn7PSLaho+z4PEbDb05CbozJAJUjdF5FJzg03qltbun3tL7UjjP9T9o/ud4hI6e2ryx0ShmTy89OGKMgnRUFOKVpJMPLvIHGsqhhNCS0mVIplOJVo/JoY7FrDeo6YygjMxIUfMmSbw3NfKv4hJlMAj8=; _dd_s=rum=0&expire=1727982830381&logs=1&id=cb7c60ee-1041-4c5d-a5f1-08f276969be9&created=1727978255603; tvl=brzlGA9M2LpnOwSHbSMB7PHig4OKsvfWuJNyEri00N2LQx+wCE65gYyguyaAZK9mI9q4eRg1+f9K4YXAzZ+qyrWKp3OBBH9mJIx/v4BmXI9QNiW1t4m1zH/svrEcP2fWYsIqSXDPA2eEmAxKcJiiZUBPe4ILqddN/hCa9vliATSfSGPZLNAQC2ZnCIHAHViXh4hyFoxsBasAAoeNN9EnMLQEInPARp5Oc+LLxsrZtLxhxvYQfhPlrXxkdUKJB6yS/ivEra7gA8U=~djAy; tvs=B9HdfWdopXj+X/ME2qslkXbDHKn+fCqfSi6gsutv89jMvzwv32bAmNFNrCm9V9yZOKdzvFasOOKr89kQ1ct+MJSYVXDFoLpuKMePUeN5gXijfmVcmgovVmu62b2LRyBrJ08/D8oU/uPaisHyDv8EYLRKldZLcY5H5ASZmHvYkSJ+HnidNHkoUwwz1yPSgWD80Z3ZJ0ZiaPyZs73pURvdfOUqAOodD7eyML22fQI8zaWGHfe3k+YPD9oUu/wLPe8rdCcpZq6SGgCEiAmfpz2eGzx8K7q4PLyjejo=~djAy',
    'origin': 'https://www.traveloka.com',
    'priority': 'u=1, i',
    'referer': 'https://www.traveloka.com/en-vn/flight/fullsearch?ap=SGN.VCA&dt=5-10-2024.NA&ps=1.0.0&sc=ECONOMY',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'x-domain': 'flight',
    'x-route-prefix': 'en-vn',
}

places = [
    ["SGN","HAN"],
    ["HAN","SGN"],
    ["SGN","VDO"],
    ["VDO","SGN"],
    ["SGN","HPH"],
    ["HPH","SGN"],
    ["SGN","VII"],
    ["VII","SGN"],
    ["SGN","HUI"],
    ["HUI","SGN"],
    ["SGN","DAD"],
    ["DAD","SGN"],
    ["SGN","CXR"],
    ["CXR","SGN"],
    ["SGN","VCA"],
    ["VCA","SGN"],
    ["SGN","PQC"],
    ["PQC","SGN"]
]

airlineDataMap = {
    "VJ": {
        "airlineId": "VJ",
        "name": "VietJet Air",
        "shortName": "VietJet Air",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2022/11/29/1669692919958-81f0c812fcaa9551ef47319232a413c0.png?tr=q-75",
        "iataCode": "VJ"
    },
    "QH": {
        "airlineId": "QH",
        "name": "Bamboo Airways",
        "shortName": "Bamboo Airways",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2020/02/19/1582084897287-d2de240a06eac5e3a70126425b62ee0b.png?tr=q-75",
        "iataCode": "QH"
    },
    "VN": {
        "airlineId": "VN",
        "name": "Vietnam Airlines",
        "shortName": "Vietnam Airlines",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2023/01/13/1673603664183-e53540c1a998ad11eab640ea454ead69.png?tr=q-75",
        "iataCode": "VN"
    },
    "BL": {
        "airlineId": "BL",
        "name": "Pacific",
        "shortName": "Pacific",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2020/12/17/1608198397864-62493eae30a8200a1cd5d0b8dcaa68cc.png?tr=q-75",
        "iataCode": "BL"
    },
    "VU": {
        "airlineId": "VU",
        "name": "Vietravel Airlines",
        "shortName": "Vietravel Airlines",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2021/03/08/1615183128719-eb20dcaed13e5b74629b222345995b7a.png?tr=q-75",
        "iataCode": "VU"
    },
    "BL": {
        "airlineId": "BL",
        "name": "Pacific",
        "shortName": "Pacific",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2020/12/17/1608198397864-62493eae30a8200a1cd5d0b8dcaa68cc.png?tr=q-75",
        "iataCode": "BL"
    },
    "MH": {
        "airlineId": "MH",
        "name": "Malaysia Airlines",
        "shortName": "Malaysia Airlines",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2022/08/10/1660130779233-6df2f0b241a6fcb657ff57cae03943e1.png?tr=q-75",
        "iataCode": "MH"
    },
    "VU": {
        "airlineId": "VU",
        "name": "Vietravel Airlines",
        "shortName": "Vietravel Airlines",
        "iconUrl": "https://ik.imagekit.io/tvlk/image/imageResource/2021/03/08/1615183128719-eb20dcaed13e5b74629b222345995b7a.png?tr=q-75",
        "iataCode": "VU"
    }
}


def load_json(path):
    """
    Read file json
    """
    with open(path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)
    json_file.close()
    return data

def write_to_json(output_path, docs):
    """ 
    Write file json
    """
    with open(output_path, 'w', encoding="utf-8") as fw:
        json.dump(docs, fw, ensure_ascii=False, indent=4)
    fw.close()

def Json_Data(month, day, year, sourceAirportOrArea, destinationAirportOrArea):
    json_data = {
        'fields': [],
        'clientInterface': 'desktop',
        'data': {
            'currency': 'VND',
            'isReschedule': False,
            'locale': 'en_VN',
            'numSeats': {
                'numAdults': 1,
                'numChildren': 0,
                'numInfants': 0,
            },
            'seatPublishedClass': 'ECONOMY',
            'destinationAirportOrArea': destinationAirportOrArea,
            'flexibleTicket': False,
            'flightDate': {
                'year': year,
                'month': month,
                'day': day,
            },
            'sourceAirportOrArea': sourceAirportOrArea,
            'newResult': True,
            'seqNo': None,
            'searchId': 'f599da5e-cb4b-46df-83d6-5b3be4ec93a1',
            'visitId': '72527839-df48-440f-bf4c-0ea46f427f70',
            'utmId': None,
            'utmSource': None,
            'searchSpecRoutesTotal': 1,
            'trackingContext': {
                'entrySource': 'date_box',
            },
            'searchSpecRouteIndex': 0,
            'journeyIndex': 0,
            'couponApplyDisabled': False,
        },
    }

    return json_data

responses= []
date = []
miss_crawl=[]
today = datetime.datetime.today().date()

for i in range(1,31):
        date.append(today + datetime.timedelta(days=i))
        
for i in range(18):
        source, destina = places[i]
        for idx, d in enumerate(date):
                month = d.month
                day = d.day
                year = d.year
                json_data = Json_Data(month =month, 
                        day= day, 
                        year = year, 
                        sourceAirportOrArea = source, 
                        destinationAirportOrArea = destina)
                try: 
                        request = requests.post('https://www.traveloka.com/api/v2/flight/search/oneway', cookies=cookies, headers=headers, json=json_data)
                        request.json()['data']['searchResults']
                except:
                        print("Waitting for you...")
                        print(i,source, destina, (d - today).days)
                        miss_crawl.append([i,source, destina, (d - today).days])
                        break
                responses.append({
                        "flight": f'{source}.{destina}',
                        "date": f"{day}-{month}-{year}",
                        "request": request
                        })
data_total = {}
for source, destina in places:
    data_total[f'{source}.{destina}'] = []

for response in responses:
    date = response["date"]
    request = response["request"]
    flight = response["flight"]
    for re in request.json()['data']['searchResults']:
        if len(re['connectingFlightRoutes']) >1:
            continue
        price_discout = re['agentFareInfo']['paxAveragePrice']
        price = re['airlineFareInfo']['detailedSearchFares'][0]['flightRouteFares']['adultAirlineFare']['amount']

        aircraftInformation = re['connectingFlightRoutes'][0]['segments'][0]['aircraftInformation']
        aircraft = aircraftInformation['aircraft']
        if aircraft != None:
            aircraft = aircraft['model']

        flightNumber = re['connectingFlightRoutes'][0]['segments'][0]['flightNumber']

        departureTime = re['connectingFlightRoutes'][0]['segments'][0]['departureTime']
        arrivalTime = re['connectingFlightRoutes'][0]['segments'][0]['arrivalTime']

        departureAirport = re['connectingFlightRoutes'][0]["departureAirport"]
        arrivalAirport = re['connectingFlightRoutes'][0]["arrivalAirport"]

        airlineFareInfo = re['connectingFlightRoutes'][0]
        BaggageInformation = re['connectingFlightRoutes'][0]['purchaseableBaggageInformation']

        Airline = airlineDataMap[re['connectingFlightRoutes'][0]['segments'][0]['airlineCode']]["name"]

        collectionDate = datetime.datetime.today().date().strftime(format='%d-%m-%Y')

        temp = {
            "price": float(price),
            "price_discount": float(price_discout),
            "departureAirport": departureAirport,
            "arrivalAirport": arrivalAirport,
            "flightNumber": flightNumber,
            "aircraft": aircraft,
            "airline":Airline,
            "departureTime": departureTime,
            "arrivalTime": arrivalTime,
            "date": date,
            "BaggageInformation": BaggageInformation,
            "collectionDate": collectionDate
        }
        data_total[flight].append(temp)

record_respose = []
for item in responses:
    record_respose.append(
        {
            "flight": item["flight"], 
            'date': item['date'], 
            'request': item['request'].json()
        }
    )

write_to_json(f'/Users/trongvox/Dropbox/Cao Học/DE/data_crawl/data_collect_{today.strftime("%y%m%d")}.json', data_total)
write_to_json(f'/Users/trongvox/Dropbox/Cao Học/DE/data_crawl/respose_{today.strftime("%y%m%d")}.json', record_respose)