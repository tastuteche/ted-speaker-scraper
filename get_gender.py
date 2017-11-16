import aiohttp
import asyncio
import async_timeout
import bs4
import asyncio
import json
import logging

import aiohttp
from aiohttp import ClientSession
from aiohttp.http_exceptions import HttpProcessingError
import aiofiles
import aioredis
# setting up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
logger.addHandler(console)
MAX_CONNECTION = 1000
URL_BASE = "https://www.ted.com"
URL_MAX_ITEM = 'https://www.ted.com/people/speakers'


async def get_page_speaker(html, queue):
    list_url = []
    soup = bs4.BeautifulSoup(html, "lxml")
    a_all = soup.findAll('a', class_='pagination__item pagination__link')
    for a in a_all:
        list_url.append(URL_BASE + a['href'])

    a_speaker_all = soup.findAll(
        'a', class_='results__result media media--sm-v m4')
    for a in a_speaker_all:
        list_url.append(URL_BASE + a['href'])
    print(list_url)
    producer = await produce(queue=queue, itr_items=list_url)


# asyncio http consumer
import os

ua = 'Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0'
headers = {
    'User-Agent': ua,
}

# proxy="http://127.0.0.1:8118",


async def consumer(main_queue, dlq, session, response_dict, redis_conn):
    while True:
        try:
            # Fetch the url from the queue, blocking until the queue has item
            url = await main_queue.get()
            print(url)
            if url in response_dict:
                main_queue.task_done()
                print('already in response_dict', url)
            else:
                val = await redis_conn.execute('get', url)
                print(type(val))
                if val is not None:
                    html = val.decode('utf-8')
                    if html == '':
                        raise HttpProcessingError('empty cache file')
                else:
                    # Try to get a response from the sever
                    async with session.get(url, timeout=10,  headers=headers) as response:
                        # Check we got a valid response
                        response.raise_for_status()
                        # append it to the responses lists
                        print('await response')
                        html = await response.text()
                        print('awaited response', len(html))
                        if html == '':
                            raise HttpProcessingError('empty response')
                    await redis_conn.execute('set', url, html)

                await get_page_speaker(html, queue=main_queue)
                # telling the queue we finished processing the massage

                response_dict[url] = html
                main_queue.task_done()

        # In case of a time in our get request/ problem with response code
        except (HttpProcessingError, asyncio.TimeoutError) as e:
            logger.debug("Problem with %s, Moving to DLQ. main_queue: (%s), dlq: (%s)|%s" %
                         (url, str(main_queue.qsize()), str(dlq.qsize())), str(e))
            # we put the url in the dlq, so other consumers wil handle it
            await dlq.put(url)
            # lower the pace
            asyncio.sleep(15)
            # telling the queue we finished processing the massage
            main_queue.task_done()


async def produce(queue, itr_items):
    for item in itr_items:
        # if the queue is full(reached maxsize) this line will be blocked
        # until a consumer will finish processing a url
        print('put url', item)
        await queue.put(item)

# proxy="http://127.0.0.1:8118",


async def download_last_n_posts(session, n, consumer_num, redis_conn):
    # We init the main_queue with a fixed size, and the dlq with unlimited size
    main_queue, dlq, response_dict = asyncio.Queue(
        maxsize=2000), asyncio.Queue(), {}
    # we init the consumers, as the queues are empty at first,
    # they will be blocked on the main_queue.get()
    consumers = [asyncio.ensure_future(
        consumer(main_queue, dlq, session, response_dict, redis_conn))
        for _ in range(consumer_num)]
    # init the dlq consumers, same as the base consumers,
    # Only main_queue is the dlq.
    dlq_consumers = [asyncio.ensure_future(
        consumer(main_queue=dlq, dlq=dlq, session=session, response_dict=response_dict, redis_conn=redis_conn))
        for _ in range(5)]
    # get the max item from the API
    async with session.get(URL_MAX_ITEM,  headers=headers) as resp:
        html = await resp.text()
    await get_page_speaker(html, queue=main_queue)

    # wait for all item's inside the main_queue to get task_done
    await main_queue.join()
    # wait for all item's inside the dlq to get task_done
    await dlq.join()
    # cancel all coroutines
    for consumer_future in consumers + dlq_consumers:
        consumer_future.cancel()
    return response_dict

import re


def get_filename(url):
    return re.sub(r'[?&=]', '__', re.sub(r'http[s]?://', '', url).replace(".", "_").replace("/", "___"))


async def run(loop, n):
    redis_conn = await aioredis.create_connection(
        ('localhost', 26379), loop=loop)
    conn_num = min(MAX_CONNECTION, n)
    # we init more connectors to get better performance
    with ClientSession(loop=loop, connector=aiohttp.TCPConnector(limit=conn_num)) as session:
        response_dict = await download_last_n_posts(session, n, conn_num, redis_conn)

import concurrent
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, n=5))
    # Zero-sleep to allow underlying connections to close
    # loop.run_until_complete(asyncio.sleep(0))
    # loop.close()
