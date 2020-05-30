1. Описание схемы данных на прикреплённой картинке
2. Входные данные необходимо проверить на наличие полного набора полей.
Ожидается, что объекты jsonа не будут содержать null и соответствуют схеме</br>
{ </br>
"type": "object",</br>
"properties": {</br>
"userId": {"type": "integer"},</br>
"platform": {"type": "string"},</br>
"durationMs": {"type": "long"},</br>
"position": {"type": "integer"},</br>
"timestamp": {"type": "long"},</br>
"owners": {</br>
"type": "object",</br>
"properties": {</br>
"user": {"type": "array", "items": [{"type": "integer"}]},</br>
"group": {"type": "array", "items": [{"type": "integer"}]}</br>
},</br>
},</br>
"resources": {</br>
"type": "object",</br>
"properties": {</br>
"GROUP_PHOTO": {"type": "array", "items": [{"type": "integer"}]</br>
},</br>
"POST": {"type": "array", "items": [{"type": "integer"}]},</br>
"MOVIE": {"type": "array", "items": [{"type": "integer"}]},</br>
"USER_PHOTO": {"type": "array", "items": [{"type": "integer"}]}</br>
}</br>
}</br>
},</br>
"required": ["userId", "platform", "durationMs", "position", "timestamp", "owners", "resources"]</br>
}</br>

3. Запросы для расчёта метрик и оценка их сложности в файле https://github.com/suhova/OkTestTask/blob/master/src/main/java/FeedShowSelects.java

![Схема данных](https://github.com/suhova/OkTestTask/blob/master/shema.jpg)
