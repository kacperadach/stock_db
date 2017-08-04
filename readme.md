- Create a service file: stock_db.service
- Put it in /lib/systemd/system/
- Reload systemd using command: systemctl daemon-reload
- Enable auto start using command: systemctl enable stock_db.service

To change stock_db.sh:

systemctl stop stock_db
vim /usr/bin/stock_db.sh
systemctl daemon-reload
systemctl start stock_db

Change time on a linux server:

timedatectl
timedatectl set-timezone 'EST'

sudo dpkg-reconfigure tzdata