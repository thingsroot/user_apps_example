## IOT Bridged MQTT Applications

### Pre-installation
1. Install MQTT server.
2. Install Redis server.
3. Install InfluxDB server.

### Installation

1. Install supervisor, python3, python3-pip
2. Checkout code to /usr/iot_user_apps
3. Install required python3 modules by: pip3 install -r requirements.txt
4. cd /etc/supervisior/conf.d && ln -s /usr/iot_user_apps/supervisior.conf ./iot_user_apps.conf
5. Reload supervisor by: sudo supervisorctrl reload

### Make it run

1. Register your account in ThingsRoot Cloud
2. Create user application in IOT Hub

 
