import my_consumer
import json
import time
import my_producer

if __name__ == '__main__':
    # 消费者
    consumer = my_consumer.my_consumer('192.168.1.214', '9092', 'demo', 'group_demo')
    # 生产者
    producer = my_producer.my_producer('192.168.1.214', '9092', 'second')

    # 建立常轮询不停拉取数据
    message = consumer.consume_data()
    # 解析数据
    for msg in message:
        # 将字符串转为json
        data = json.loads(str(msg.value, encoding='utf-8'))
        # 做json结构调整
        data['common'] = dict()
        data['location'] = dict()
        data['common']['version'] = data['version']
        data['location']['lat'] = float(data['latitude'])
        data['location']['lng'] = float(data['longtitude'])
        data['timestampMs'] = int(time.mktime(time.strptime(data['log_time'], "%Y-%m-%d %H:%M:%S"))) * 1000

        # 做业务逻辑处理
        user_agent = data['client']['user_agent']
        if data['app_type'] == 100:
            if user_agent == '' or user_agent is None:
                data['common']['platform'] = 'H5'
                data['common']['os'] = 'UNKNOWN'
            elif user_agent == 'IOS':
                data['common']['platform'] = 'IOS'
                data['common']['os'] = 'IOS'
            elif user_agent == 'Andorid':
                data['common']['platform'] = 'Andorid'
                data['common']['os'] = 'Andorid'

        # 删除不需要的元素
        del data['client']['user_agent'], data['latitude'], data['longtitude'], data['log_time']

        # 按对应分区往kafka发送data
        producer.send_data(data, msg.partition)

        print(data)
