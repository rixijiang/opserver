# 导入 socket、sys 模块
from module.socketdo import SendData
from module.constant import CON_SEND
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 测试一发送一段信息
buffer = 'hello world!'

# 发送信息
# SendData(CON_SEND['ADDR'], CON_SEND['BUFFSIZE'], buffer)


print(tuple([1,2,3,3,54]))
