#!/usr/bin/bash python3
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from urllib import parse
import psycopg2
import json, time
import requests
import logging

http_server_host = '0.0.0.0'
http_server_port = 18084

class GetHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        o = parse.urlparse(self.path)
        query = parse.parse_qs(o.query)
        logging.info(query)

        livy_addr = query['livy_addr'][0]
        session_id = query['session_id'][0]
        dataframe = query['dataframe'][0]

        livy_api = f'http://{livy_addr}/sessions/{session_id}/statements'

        data = {'code': f'{dataframe}.show()', 'kind': 'pyspark'}
        r = requests.post(livy_api, json.dumps(data))
        statement_id = r.json()['id']
        while True:
            livy_api1 = livy_api + '/' + str(statement_id)
            logging.info("post livy_api: %s", livy_api)
            r = requests.get(livy_api1)
            logging.info(r.json())
            if r.json()['state'] != 'running':
                break
            else:
                time.sleep(1/10)

        self.send_response(r.status_code)
        self.send_header('Content-Type',
                'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(r.json()['output']['data']).encode('utf-8'))

def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:(%(thread)d)%(asctime)s - %(message)s')
    server = HTTPServer((http_server_host, http_server_port), GetHandler)
    print(f'Starting server {http_server_host}:{http_server_port}, use <Ctrl-C> to stop')
    try:
        server.serve_forever()
    except Exception as e:
        print('server closed!')

if __name__ == '__main__':
    main()
