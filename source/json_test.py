# client
"""
client.py
"""
import socket
import json

if __name__ == '__main__':
    num = 10
    try:
        print('클라이언트 동작')
        # initialize Socket
        SERVER_ADDR = ('192.168.0.4', 7001)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(SERVER_ADDR)
            print('서버에 연결 되었습니다.')

            while True:
                # input_tmp = input('전송할 데이터를 입력하세요:')
                # data = {'name': 'client', 'contents': input_tmp, 'num': num}
                # print("send: name: {0}, contents: {1}, num: {2}".format(data['name'], data['contents'], data['num']))
                # client.sendall(json.dumps(data).encode('UTF-8'))

                data = client.recv(16384)
                print(data)
                data = json.loads(data)  
                print("receive: name: {0}, contents: {1}, num: {2}".format(data['name'], data['contents'], data['num']))

                num = int(data['num']) + 1

    except Exception as e:
        print(e)
        input_tmp = input('아무거나 눌러서 종료')