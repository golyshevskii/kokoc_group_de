#### Процессы извлечения, трансформации и загрузки данных астероидов за 3 дня в файлы и DWH
----
###### Мануал пользователя
1. Скачайте репозиторий
```
git clone https://github.com/golyshevskii/kokoc_group_de.git
```
2. Установите виртуальную среду
```
python -m venv venv
```
3. Активируйте виртуальную среду разработки
```
source venv/bin/activate
```
4. Установите необходимые библиотеки
```
pip install -r requirements.txt
```
5. Создайте файл **`credentials.py`**, в котором задайте две переменные:
```
NASA_API = 'токен подключения к API'
DWH_PASSWORD = 'пароль для подключения к БД'
# также в переменной conn, в теле функци dwh_connection, укажите данные для подключения к вашей БД
```
6. Запустите скрипт python через терминал
```
python etl_near_earth_objects.py >> etl_log.log
```
